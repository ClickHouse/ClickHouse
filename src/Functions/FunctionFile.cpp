#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <Access/Common/AccessFlags.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Disks/IVolume.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <filesystem>
#include <Functions/FunctionHelpers.h>
#include <Core/ColumnWithTypeAndName.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int DATABASE_ACCESS_DENIED;
}

namespace
{

bool isStringOrNull(const IDataType & type)
{
    return isString(type) || type.onlyNull();
}

}

/// A function to read file as a string.
class FunctionFile : public IFunction
{
public:
    static constexpr auto name = "file";
    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getApplicationType() != Context::ApplicationType::LOCAL)
            context->checkAccess(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE));

        return std::make_shared<FunctionFile>();
    }

    bool isVariadic() const override { return true; }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"path", &isString, nullptr, "String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"default", &isStringOrNull, nullptr, "String or Null"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        auto ret = std::make_shared<DataTypeString>();
        if (arguments.size() == 2 && arguments[1].type->onlyNull())
            return makeNullable(ret);
        return ret;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnString * column_src = checkAndGetColumn<ColumnString>(column.get());
        if (!column_src)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        String default_result;

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;

        if (arguments.size() == 2)
        {
            if (result_type->isNullable())
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                vec_null_map_to = &col_null_map_to->getData();
            }
            else
            {
                const auto & default_column = arguments[1].column;
                const ColumnConst * default_col = checkAndGetColumn<ColumnConst>(default_column.get());

                if (!default_col)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[1].column->getName(), getName());

                default_result = default_col->getValue<String>();
            }
        }

        auto result = ColumnString::create();
        auto & res_chars = result->getChars();
        auto & res_offsets = result->getOffsets();

        res_offsets.resize(input_rows_count);

        const auto & context = Context::getGlobalContextInstance();
        auto user_files_volume = context->getUserFilesVolume();

        /// When user_files_policy is set, use disk-based I/O
        if (user_files_volume)
        {
            auto disks = user_files_volume->getDisks();

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                std::string_view filename = column_src->getDataAt(row);
                String file_path_str(filename);

                /// Remove leading slash for disk-relative paths
                if (!file_path_str.empty() && file_path_str[0] == '/')
                    file_path_str = file_path_str.substr(1);

                try
                {
                    /// Find the file on one of the disks
                    DiskPtr found_disk;
                    for (const auto & disk : disks)
                    {
                        if (disk->existsFile(file_path_str))
                        {
                            found_disk = disk;
                            break;
                        }
                    }

                    if (!found_disk)
                        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File not found on any user files disk");

                    auto read_settings = context->getReadSettings();
                    auto in = found_disk->readFile(file_path_str, read_settings);
                    auto out = WriteBufferFromVector<ColumnString::Chars>(res_chars, AppendModeTag{});
                    copyData(*in, out);
                }
                catch (...)
                {
                    if (arguments.size() == 1)
                        throw;

                    if (vec_null_map_to)
                        (*vec_null_map_to)[row] = true;
                    else
                        res_chars.insert(default_result.data(), default_result.data() + default_result.size());
                }

                res_offsets[row] = res_chars.size();
            }
        }
        else
        {
        Strings user_files_paths = context->getUserFilesPaths();
        /// Compute canonical paths for each user_files_path
        std::vector<std::string> user_files_absolute_paths;
        for (const auto & ufp : user_files_paths)
            user_files_absolute_paths.push_back(fs::canonical(fs::path(ufp)).string());

        // If run in Local mode, no need for path checking.
        bool need_check = context->getApplicationType() != Context::ApplicationType::LOCAL;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view filename = column_src->getDataAt(row);
            fs::path file_path(filename.data(), filename.data() + filename.size());

            if (file_path.is_relative())
            {
                /// For relative paths, try each user_files_path and use the first one where the file exists.
                /// If not found on any, fall back to the first path.
                bool found = false;
                for (const auto & ufp : user_files_absolute_paths)
                {
                    fs::path candidate = fs::absolute(fs::path(ufp) / file_path).lexically_normal();
                    if (fs::exists(candidate))
                    {
                        file_path = candidate;
                        found = true;
                        break;
                    }
                }
                if (!found)
                    file_path = fs::absolute(fs::path(user_files_absolute_paths.front()) / file_path).lexically_normal();
            }

            /// Do not use fs::canonical or fs::weakly_canonical.
            /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
            file_path = fs::absolute(file_path).lexically_normal();

            try
            {
                if (need_check)
                {
                    bool inside = false;
                    for (const auto & ufp : user_files_absolute_paths)
                    {
                        if (file_path.string().starts_with(ufp))
                        {
                            inside = true;
                            break;
                        }
                    }
                    if (!inside)
                        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File is not inside user files path");
                }

                ReadBufferFromFile in(file_path);
                auto out = WriteBufferFromVector<ColumnString::Chars>(res_chars, AppendModeTag{});
                copyData(in, out);
            }
            catch (...)
            {
                if (arguments.size() == 1)
                    throw;

                if (vec_null_map_to)
                    (*vec_null_map_to)[row] = true;
                else
                    res_chars.insert(default_result.data(), default_result.data() + default_result.size());
            }

            res_offsets[row] = res_chars.size();
        }
        } /// end of else (no user_files_volume)

        if (vec_null_map_to)
            return ColumnNullable::create(std::move(result), std::move(col_null_map_to));

        return result;
    }
};


REGISTER_FUNCTION(File)
{
    FunctionDocumentation::Description description = R"(
Reads a file as a string and loads the data into the specified column.
The file content is not interpreted.

Also see the [`file`](../table-functions/file.md) table function.
        )";
    FunctionDocumentation::Syntax syntax = "file(path[, default])";
    FunctionDocumentation::Arguments arguments = {
        {"path", "The path of the file relative to the `user_files_path`. Supports wildcards `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` are numbers and `'abc', 'def'` are strings.", {"String"}},
        {"default", "The value returned if the file does not exist or cannot be accessed.", {"String", "NULL"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the file content as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Insert files into a table",
            R"(
INSERT INTO table SELECT file('a.txt'), file('b.txt');
            )",
            R"(
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFile>(documentation);
}

}
