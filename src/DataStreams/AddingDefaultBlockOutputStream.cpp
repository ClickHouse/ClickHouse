#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

AddingDefaultBlockOutputStream::AddingDefaultBlockOutputStream(
    const BlockOutputStreamPtr & output_,
    const Block & header_,
    const ColumnsDescription & columns_,
    const Context & context_)
    : output(output_), header(header_)
{
    auto dag = addMissingDefaults(header_, output->getHeader().getNamesAndTypesList(), columns_, context_);
    adding_defaults_actions = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings::fromContext(context_));
}

void AddingDefaultBlockOutputStream::write(const Block & block)
{
    auto copy = block;
    adding_defaults_actions->execute(copy);
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
