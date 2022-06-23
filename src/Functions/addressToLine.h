#pragma once
#if defined(__ELF__) && !defined(OS_FREEBSD)

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


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename ResultT, Dwarf::LocationInfoMode locationInfoMode>
class FunctionAddressToLineBase : public IFunction
{
public:

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                "Function " + getName() + " needs exactly one argument; passed " + toString(arguments.size()) + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isUInt64())
            throw Exception(
                "The only argument for function " + getName() + " must be UInt64. Found " + type->getName() + " instead.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return getDataType();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());

        if (!column_concrete)
            throw Exception(
                "Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
        return getResultColumn(data, input_rows_count);
    }

protected:
    virtual DataTypePtr getDataType() const = 0;
    virtual ColumnPtr getResultColumn(const typename ColumnVector<UInt64>::Container & data, size_t input_rows_count) const = 0;
    virtual void
    setResult(ResultT & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> & frames) const = 0;

    struct Cache
    {
        std::mutex mutex;
        Arena arena;
        using Map = HashMap<uintptr_t, ResultT>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;
    };

    mutable Cache cache;

    ResultT impl(uintptr_t addr) const
    {
        auto symbol_index_ptr = SymbolIndex::instance();
        const SymbolIndex & symbol_index = *symbol_index_ptr;

        if (const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr)))
        {
            auto dwarf_it = cache.dwarfs.try_emplace(object->name, object->elf).first;
            if (!std::filesystem::exists(object->name))
                return {};

            Dwarf::LocationInfo location;
            std::vector<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
            ResultT result;
            if (dwarf_it->second.findAddress(addr - uintptr_t(object->address_begin), location, locationInfoMode, frames))
            {
                setResult(result, location, frames);
                return result;
            }
            else
                return {object->name};
        }
        else
            return {};
    }

    ResultT implCached(uintptr_t addr) const
    {
        typename Cache::Map::LookupResult it;
        bool inserted;
        std::lock_guard lock(cache.mutex);
        cache.map.emplace(addr, it, inserted);
        if (inserted)
            it->getMapped() = impl(addr);
        return it->getMapped();
    }
};

}

#endif
