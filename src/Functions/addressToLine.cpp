#if defined(__ELF__) && !defined(__FreeBSD__)

#include <Common/Dwarf.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessFlags.h>

#include <Functions/addressToLine.h>


namespace DB
{

namespace
{

class FunctionAddressToLine: public FunctionAddressToLineBase<StringRef, Dwarf::LocationInfoMode::FAST>
{
public:
    static constexpr auto name = "addressToLine";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLine);
        return std::make_shared<FunctionAddressToLine>();
    }
protected:
    DataTypePtr getDataType() const override
    {
        return std::make_shared<DataTypeString>();
    }
    ColumnPtr getResultColumn(const typename ColumnVector<UInt64>::Container & data, size_t input_rows_count) const override
    {
        auto result_column = ColumnString::create();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef res_str = implCached(data[i]);
            result_column->insertData(res_str.data, res_str.size);
        }
        return result_column;
    }

    void setResult(StringRef & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> &) const override
    {
        const char * arena_begin = nullptr;
        WriteBufferFromArena out(cache.arena, arena_begin);

        writeString(location.file.toString(), out);
        writeChar(':', out);
        writeIntText(location.line, out);

        result = out.complete();
    }
};

}

void registerFunctionAddressToLine(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddressToLine>();
}

}

#endif
