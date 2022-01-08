#if defined(__ELF__) && !defined(__FreeBSD__)

#include <Common/Dwarf.h>
#include <Common/SymbolIndex.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Arena.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>

#include <mutex>
#include <filesystem>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{


class FunctionAddressToLineWithInlines : public IFunction
{
public:
    static constexpr auto name = "addressToLineWithInlines";
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLineWithInlines);
        return std::make_shared<FunctionAddressToLineWithInlines>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " needs exactly one argument; passed "
                + toString(arguments.size()) + ".", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isUInt64())
            throw Exception("The only argument for function " + getName() + " must be UInt64. Found "
                + type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());

        if (!column_concrete)
            throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
        auto result_column = ColumnArray::create(ColumnString::create());

        ColumnString & result_strings = typeid_cast<ColumnString &>(result_column->getData());
        ColumnArray::Offsets & result_offsets = result_column->getOffsets();

        ColumnArray::Offset current_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRefs res = implCached(data[i]);
            for (auto & r : res)
                result_strings.insertData(r.data, r.size);
            current_offset += res.size();
            result_offsets.push_back(current_offset);
        }

        return result_column;
    }

private:
    struct Cache
    {
        std::mutex mutex;
        Arena arena;
        using Map = HashMap<uintptr_t, StringRefs>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;
    };

    mutable Cache cache;

    inline ALWAYS_INLINE void appendLocation2Result(StringRefs & result, Dwarf::LocationInfo & location, Dwarf::SymbolizedFrame * frame) const
    {
        const char * arena_begin = nullptr;
        WriteBufferFromArena out(cache.arena, arena_begin);

        writeString(location.file.toString(), out);
        writeChar(':', out);
        writeIntText(location.line, out);

        if (frame)
        {
            writeChar(':', out);
            int status = 0;
            writeString(demangle(frame->name, status), out);
        }

        result.emplace_back(out.complete());
    }

    StringRefs impl(uintptr_t addr) const
    {
        auto symbol_index_ptr = SymbolIndex::instance();
        const SymbolIndex & symbol_index = *symbol_index_ptr;

        if (const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr)))
        {
            auto dwarf_it = cache.dwarfs.try_emplace(object->name, object->elf).first;
            if (!std::filesystem::exists(object->name))
                return {};

            Dwarf::LocationInfo location;
            std::vector<Dwarf::SymbolizedFrame> inline_frames;
            if (dwarf_it->second.findAddress(addr - uintptr_t(object->address_begin), location, Dwarf::LocationInfoMode::FULL_WITH_INLINE, inline_frames))
            {
                StringRefs ret;
                appendLocation2Result(ret, location, nullptr);
                for (auto & inline_frame : inline_frames)
                    appendLocation2Result(ret, inline_frame.location, &inline_frame);
                return ret;
            }
            else
            {
                return {object->name};
            }
        }
        else
            return {};
    }

    /// ALWAYS_INLINE is also a self-containing testcase used in 0_stateless/02161_addressToLineWithInlines.
    /// If changed here, change 02161 together.
    inline ALWAYS_INLINE StringRefs implCached(uintptr_t addr) const
    {
        Cache::Map::LookupResult it;
        bool inserted;
        std::lock_guard lock(cache.mutex);
        cache.map.emplace(addr, it, inserted);
        if (inserted)
            it->getMapped() = impl(addr);
        return it->getMapped();
    }
};

}

void registerFunctionAddressToLineWithInlines(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddressToLineWithInlines>();
}

}

#endif
