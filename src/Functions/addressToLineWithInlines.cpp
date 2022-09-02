#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Common/Dwarf.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessFlags.h>

#include <Functions/addressToLine.h>
#include <vector>


namespace DB
{

namespace
{

class FunctionAddressToLineWithInlines: public FunctionAddressToLineBase<StringRefs, Dwarf::LocationInfoMode::FULL_WITH_INLINE>
{
public:
    static constexpr auto name = "addressToLineWithInlines";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLineWithInlines);
        return std::make_shared<FunctionAddressToLineWithInlines>();
    }

protected:
    DataTypePtr getDataType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr getResultColumn(const typename ColumnVector<UInt64>::Container & data, size_t input_rows_count) const override
    {
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

    void setResult(StringRefs & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> & inline_frames) const override
    {

        appendLocationToResult(result, location, nullptr);
        for (const auto & inline_frame : inline_frames)
            appendLocationToResult(result, inline_frame.location, &inline_frame);
    }
private:

    inline ALWAYS_INLINE void appendLocationToResult(StringRefs & result, const Dwarf::LocationInfo & location, const Dwarf::SymbolizedFrame * frame) const
    {
        const char * arena_begin = nullptr;
        WriteBufferFromArena out(cache.arena, arena_begin);

        writeString(location.file.toString(), out);
        writeChar(':', out);
        writeIntText(location.line, out);

        if (frame && frame->name != nullptr)
        {
            writeChar(':', out);
            int status = 0;
            writeString(demangle(frame->name, status), out);
        }

        result.emplace_back(out.complete());
    }

};

}

REGISTER_FUNCTION(AddressToLineWithInlines)
{
    factory.registerFunction<FunctionAddressToLineWithInlines>();
}

}

#endif
