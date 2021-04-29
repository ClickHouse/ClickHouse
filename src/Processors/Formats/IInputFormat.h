#pragma once

#include <Processors/ISource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ReadBuffer;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public ISource
{
protected:

    /// Skip GCC warning: ‘maybe_unused’ attribute ignored
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"

    ReadBuffer & in [[maybe_unused]];

#pragma GCC diagnostic pop

public:
    IInputFormat(Block header, ReadBuffer & in_);

    /** In some usecase (hello Kafka) we need to read a lot of tiny streams in exactly the same format.
     * The recreating of parser for each small stream takes too long, so we introduce a methodа 
     * resetParser() which allow to reset the state of parser to continue reading of
     * source stream w/o recreating that.
     * That should be called after current buffer was fully read.
     */
    virtual void resetParser();

    virtual const BlockMissingValues & getMissingValues() const
    {
        static const BlockMissingValues none;
        return none;
    }

    virtual Block readSchemaFromPrefix()
    {
        throw Exception("readSchemaFromPrefix is not implemented for" + getName() + " InputFormat", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t getCurrentUnitNumber() const { return current_unit_number; }
    void setCurrentUnitNumber(size_t current_unit_number_) { current_unit_number = current_unit_number_; }

private:
    /// Number of currently parsed chunk (if parallel parsing is enabled)
    size_t current_unit_number = 0;
};

}
