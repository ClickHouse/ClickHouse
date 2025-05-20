#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>

#include <Common/logger_useful.h>

namespace DB
{

template <typename Base>
class OutputFormatWithUTF8ValidationAdaptorBase : public Base
{
public:
    OutputFormatWithUTF8ValidationAdaptorBase(const Block & header, WriteBuffer & out_, bool validate_utf8)
        : Base(header, out_)
    {
        bool values_can_contain_invalid_utf8 = false;
        for (const auto & type : this->getPort(IOutputFormat::PortKind::Main).getHeader().getDataTypes())
        {
            if (!type->textCanContainOnlyValidUTF8())
                values_can_contain_invalid_utf8 = true;
        }

        if (validate_utf8 && values_can_contain_invalid_utf8)
            validating_ostr = std::make_unique<WriteBufferValidUTF8>(*Base::getWriteBufferPtr());
    }

    void flush() override
    {
        if (validating_ostr)
            validating_ostr->next();
        Base::flush();
    }

    void finalizeBuffers() override
    {
        if (validating_ostr)
            validating_ostr->finalize();
        Base::finalizeBuffers();
    }

    void resetFormatterImpl() override
    {
        Base::resetFormatterImpl();
        if (validating_ostr)
            validating_ostr = std::make_unique<WriteBufferValidUTF8>(*Base::getWriteBufferPtr());
    }

protected:
    /// Returns buffer that should be used in derived classes instead of out.
    WriteBuffer * getWriteBufferPtr() override
    {
        if (validating_ostr)
            return validating_ostr.get();
        return Base::getWriteBufferPtr();
    }

private:
    /// Validates UTF-8 sequences, replaces bad sequences with replacement character.
    std::unique_ptr<WriteBuffer> validating_ostr;
};

using OutputFormatWithUTF8ValidationAdaptor = OutputFormatWithUTF8ValidationAdaptorBase<IOutputFormat>;
using RowOutputFormatWithUTF8ValidationAdaptor = OutputFormatWithUTF8ValidationAdaptorBase<IRowOutputFormat>;

}

