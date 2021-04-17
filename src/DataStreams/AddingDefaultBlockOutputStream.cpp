#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <Interpreters/addMissingDefaults.h>


namespace DB
{

void AddingDefaultBlockOutputStream::write(const Block & block)
{
    output->write(addMissingDefaults(block, output_block.getNamesAndTypesList(), columns, context));
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
