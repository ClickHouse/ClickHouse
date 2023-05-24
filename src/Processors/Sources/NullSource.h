#pragma once
#include <Processors/ISource.h>
#include <Common/logger_useful.h>

namespace DB
{

class NullSource : public ISource
{
public:
    explicit NullSource(Block header) : ISource(std::move(header)) {}
    String getName() const override { return "NullSource"; }

protected:
    Chunk generate() override { 
        LOG_FATAL(&Poco::Logger::root(), "NullSource: '{}'.", "generate");
        return Chunk(); }
};

}
