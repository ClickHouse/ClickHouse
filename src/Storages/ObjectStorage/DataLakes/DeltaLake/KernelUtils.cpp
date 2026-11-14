#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#if USE_DELTA_KERNEL_RS
#include "delta_kernel_ffi.hpp"


#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Processors/Chunk.h>
#include <Interpreters/castColumn.h>
#include <IO/WriteHelpers.h>

#include <Poco/String.h>
#include <fmt/ranges.h>
#include <filesystem>


namespace DB::ErrorCodes
{
    extern const int DELTA_KERNEL_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

namespace DeltaLake
{

std::string generateWritePath(const std::string & prefix, const std::string & format_str)
{
    return std::filesystem::path(prefix) / (DB::toString(DB::UUIDHelpers::generateV4()) + "." + Poco::toLower(format_str));
}

DB::SharedHeader makeDeltaWriteHeader(const DB::Block & sample, const DB::NamesAndTypesList & write_schema)
{
    DB::Block header = sample;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        auto & col = header.getByPosition(i);
        auto schema_col = write_schema.tryGetByName(col.name);
        if (schema_col && !schema_col->type->equals(*col.type))
        {
            col.type = schema_col->type;
            col.column = col.type->createColumn();
        }
    }
    return std::make_shared<const DB::Block>(std::move(header));
}

DB::Chunk castChunkToDeltaWriteSchema(const DB::Chunk & chunk, const DB::Block & in_header, const DB::Block & out_header, bool accurate)
{
    const auto & in_columns = chunk.getColumns();
    DB::Columns out_columns;
    out_columns.reserve(in_columns.size());
    for (size_t i = 0; i < in_columns.size(); ++i)
    {
        const auto & from = in_header.getByPosition(i);
        const auto & to_type = out_header.getByPosition(i).type;
        if (from.type->equals(*to_type))
            out_columns.push_back(in_columns[i]);
        else
            out_columns.push_back(
                accurate
                    ? DB::castColumnAccurate({in_columns[i], from.type, from.name}, to_type)
                    : DB::castColumn({in_columns[i], from.type, from.name}, to_type));
    }
    return DB::Chunk(std::move(out_columns), chunk.getNumRows());
}

ffi::KernelStringSlice KernelUtils::toDeltaString(const std::string & string)
{
    return ffi::KernelStringSlice{ .ptr = string.data(), .len = string.size() };
}

std::string KernelUtils::fromDeltaString(ffi::KernelStringSlice slice)
{
    return std::string(slice.ptr, slice.len);
}

void * KernelUtils::allocateString(ffi::KernelStringSlice slice)
{
    return new std::string(slice.ptr, slice.len);
}

namespace
{

std::mutex mutex;
std::unordered_map<uintptr_t, std::string> allocated_errors;

uintptr_t ptrToInt(void * error)
{
    return reinterpret_cast<uintptr_t>(error);
}

void recordKernelErrorAllocation(uintptr_t error, std::string error_message)
{
    std::lock_guard guard(mutex);

    const auto [it, inserted] = allocated_errors.emplace(error, error_message);

    if (!inserted)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Trying to record already allocated error. (Current Message: {}, Previous Message: {})",
            error_message, it->second);
    }
}

std::string forgetKernelErrorAllocation(uintptr_t error)
{
    std::lock_guard guard(mutex);

    const auto it = allocated_errors.find(error);
    if (it == allocated_errors.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Trying to deallocate unregistered error");

    std::string recorded_message = std::move(it->second);
    allocated_errors.erase(it);

    return recorded_message;
}

bool isKernelErrorAllocation(uintptr_t error)
{
    std::lock_guard guard(mutex);
    return allocated_errors.contains(error);
}

}

ffi::EngineError * KernelUtils::allocateError(ffi::KernelError etype, ffi::KernelStringSlice message)
{
    auto * error = new ffi::EngineError;
    error->etype = etype;

    std::string error_message(message.ptr, message.len);
    recordKernelErrorAllocation(ptrToInt(error), error_message);

    LOG_TRACE(
        getLogger("KernelUtils"),
        "Allocated KernelError (EType: {}, Message: {})",
        etype, error_message);

    return error;
}

[[noreturn]] void KernelUtils::throwError(ffi::EngineError * error, const std::string & from)
{
    if (error == nullptr)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unpacking nullptr DeltaLake kernel error. (in: {})", from);

    uintptr_t error_ptr = ptrToInt(error);

    if (isKernelErrorAllocation(error_ptr))
    {
        LOG_TRACE(
            getLogger("KernelUtils"),
            "Deallocating KernelError (in {})",
            from);

        ffi::KernelError etype = error->etype;
        delete error;

        std::string error_message = forgetKernelErrorAllocation(error_ptr);

        throw DB::Exception(
            DB::ErrorCodes::DELTA_KERNEL_ERROR,
            "Received DeltaLake kernel error {}: {} (in {})",
            etype, error_message, from);
    }
    else
    {
        ffi::KernelError etype = error->etype;
        delete error;

        LOG_ERROR(
            getLogger("KernelUtils"),
            "Received unknown error from DeltaLake kernel. (EType: {}) (in {})",
            etype, from);

        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Received unknown error from DeltaLake kernel. (EType: {}) (in {})",
            etype, from);
    }
}

std::string appendToLogicalPath(const std::string & logical_path, const std::string & field_name)
{
    std::string result = logical_path;
    if (!result.empty())
        result += '.';
    for (char c : field_name)
    {
        if (c == '.' || c == '\\')
            result += '\\';
        result += c;
    }
    return result;
}

std::optional<std::string> tryGetPhysicalName(const std::string & name, const DB::NameToNameMap & physical_names_map)
{
    if (physical_names_map.empty())
        return name;

    auto it = physical_names_map.find(name);
    if (it == physical_names_map.end())
        return std::nullopt;
    return it->second;
}

std::string getPhysicalName(const std::string & name, const DB::NameToNameMap & physical_names_map)
{
    auto physical_name = tryGetPhysicalName(name, physical_names_map);
    if (!physical_name)
    {
        DB::Names keys;
        keys.reserve(physical_names_map.size());
        for (const auto & [key, _] : physical_names_map)
            keys.push_back(key);

        /// The map comes from external Delta metadata; the name comes from the ClickHouse
        /// schema. A miss is external-schema drift (catchable), not an internal invariant.
        throw DB::Exception(
            DB::ErrorCodes::INCORRECT_DATA,
            "Column {} is not present in the DeltaLake column mapping. There are only columns: {}. "
            "The column may have been renamed or dropped in the Delta table",
            name, fmt::join(keys, ", "));
    }
    return *physical_name;
}

DB::DataTypePtr replaceTypeNamesToPhysicalRecursively(
    const DB::DataTypePtr & type,
    const std::string & parent_logical_name,
    const std::string & parent_physical_name,
    const DB::NameToNameMap & physical_names_map)
{
    if (physical_names_map.empty())
        return type;

    /// The fields of a struct inside an array or a map have physical names of their own. The schema visitor
    /// stores them under the special names the kernel gives to an array element and to a map key and value,
    /// and, as the element, key and value have no physical names, without a physical parent prefix.
    if (const auto * array_type = typeid_cast<const DB::DataTypeArray *>(type.get()))
    {
        return std::make_shared<DB::DataTypeArray>(replaceTypeNamesToPhysicalRecursively(
            array_type->getNestedType(), appendToLogicalPath(parent_logical_name, "array_element"), "", physical_names_map));
    }

    if (const auto * map_type = typeid_cast<const DB::DataTypeMap *>(type.get()))
    {
        return std::make_shared<DB::DataTypeMap>(
            replaceTypeNamesToPhysicalRecursively(
                map_type->getKeyType(), appendToLogicalPath(parent_logical_name, "map_key"), "", physical_names_map),
            replaceTypeNamesToPhysicalRecursively(
                map_type->getValueType(), appendToLogicalPath(parent_logical_name, "map_value"), "", physical_names_map));
    }

    const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(type.get());
    if (!tuple_type || !tuple_type->hasExplicitNames())
        return type;

    const auto & element_names = tuple_type->getElementNames();
    const auto & elements = tuple_type->getElements();

    DB::Names result_element_names;
    DB::DataTypes result_elements;
    result_element_names.resize(element_names.size());
    result_elements.resize(element_names.size());

    for (size_t i = 0; i < element_names.size(); ++i)
    {
        const auto & element_name = element_names[i];
        const auto full_element_name = appendToLogicalPath(parent_logical_name, element_name);

        auto physical_name = tryGetPhysicalName(full_element_name, physical_names_map);
        /// full_child_physical_path: the complete "parent.child" physical path as stored in the
        /// map (e.g. "d2.inner" or "d2.inner.a.foo"). Passed to recursive calls so that deeper
        /// levels can correctly strip *their* parent prefix regardless of nesting depth.
        std::string full_child_physical_path;
        if (physical_name)
        {
            full_child_physical_path = *physical_name;
            /// The map value is the full physical path "parent_physical.child_physical".
            /// Strip the parent prefix to get just the child's local physical name.
            /// We cannot use find_last_of('.') because the child physical name may itself
            /// contain dots (e.g. column mapping "name" mode where logical name "a.foo" is
            /// also used as the physical name).
            if (!parent_physical_name.empty() && physical_name->size() > parent_physical_name.size() + 1)
                result_element_names[i] = physical_name->substr(parent_physical_name.size() + 1);
            else
                result_element_names[i] = *physical_name;
        }
        else
        {
            /// Field not in the map (no column mapping). Reconstruct a best-effort full path
            /// so children at deeper levels still have a correct prefix to strip against.
            full_child_physical_path = parent_physical_name.empty()
                ? element_name
                : parent_physical_name + "." + element_name;
            result_element_names[i] = element_name;
        }

        const auto & child_type = elements[i];
        result_elements[i] = replaceTypeNamesToPhysicalRecursively(child_type, full_element_name, full_child_physical_path, physical_names_map);
    }
    return std::make_shared<DB::DataTypeTuple>(result_elements, result_element_names);
}

}

#endif
