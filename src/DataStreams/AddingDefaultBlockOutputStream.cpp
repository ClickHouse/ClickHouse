#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

AddingDefaultBlockOutputStream::AddingDefaultBlockOutputStream(
    const Block & in_header,
    const Block & out_header,
    const ColumnsDescription & columns_,
    ContextPtr context_,
    bool null_as_default_)
    : output(output_), header(header_)
{
    auto dag = addMissingDefaults(in_header, output_header.getNamesAndTypesList(), columns_, context_, null_as_default_);
    adding_defaults_actions = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings::fromContext(context_, CompileExpressions::yes));
}

void AddingDefaultBlockOutputStream::write(const Block & block)
{
    auto copy = block;
    adding_defaults_actions->execute(copy);
    output->write(copy);
}



}
