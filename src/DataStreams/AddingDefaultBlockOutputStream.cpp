#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

AddingDefaultBlockOutputStream::AddingDefaultBlockOutputStream(
    const BlockOutputStreamPtr & output_,
    const Block & header_,
    const Block & output_block_,
    const ColumnsDescription & columns_,
    const Context & context_)
    : output(output_), header(header_)
{
    auto dag = addMissingDefaults(header_, output_block_.getNamesAndTypesList(), columns_, context_);
    actions = std::make_shared<ExpressionActions>(std::move(dag));
}

void AddingDefaultBlockOutputStream::write(const Block & block)
{
    auto copy = block;
    actions->execute(copy);
    output->write(copy);
}

void AddingDefaultBlockOutputStream::flush()
{
    output->flush();
}

void AddingDefaultBlockOutputStream::writePrefix()
{
    output->writePrefix();
}

void AddingDefaultBlockOutputStream::writeSuffix()
{
    output->writeSuffix();
}

}
