#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>

namespace DB
{

template <typename Base, typename... Args>
class OutputFormatWithUTF8ValidationAdaptorBase : public Base
{
public:
    OutputFormatWithUTF8ValidationAdaptorBase(bool validate_utf8, const Block & header, WriteBuffer & out_, Args... args)
        : Base(header, out_, std::forward<Args>(args)...)
    {
        bool values_can_contain_invalid_utf8 = false;
        for (const auto & type : this->getPort(IOutputFormat::PortKind::Main).getHeader().getDataTypes())
        {
            if (!type->textCanContainOnlyValidUTF8())
                values_can_contain_invalid_utf8 = true;
        }

        if (validate_utf8 && values_can_contain_invalid_utf8)
        {
            validating_ostr = std::make_unique<WriteBufferValidUTF8>(this->out);
            ostr = validating_ostr.get();
        }
        else
            ostr = &this->out;
    }

    void flush() override
    {
        ostr->next();

        if (validating_ostr)
            this->out.next();
    }

    void finalizeBuffers() override
    {
        if (validating_ostr)
            validating_ostr->finalize();
    }

    void resetFormatterImpl() override
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(this->out);
        ostr = validating_ostr.get();
    }

protected:
    /// Point to validating_ostr or out from IOutputFormat, should be used in derived classes instead of out.
    WriteBuffer * ostr;

private:
    /// Validates UTF-8 sequences, replaces bad sequences with replacement character.
    std::unique_ptr<WriteBuffer> validating_ostr;
};

using OutputFormatWithUTF8ValidationAdaptor = OutputFormatWithUTF8ValidationAdaptorBase<IOutputFormat>;
using RowOutputFormatWithUTF8ValidationAdaptor = OutputFormatWithUTF8ValidationAdaptorBase<IRowOutputFormat>;

}

