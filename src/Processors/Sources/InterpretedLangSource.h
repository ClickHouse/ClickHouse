#pragma once

#include <QueryPipeline/Pipe.h>
#include <Core/Block.h>
#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{

class InterpretedLangSourceCoordinator
{
public:
    // !! Use ShellCommand or create own command executor.
    // !! use shared memory to pass args and result?

    struct Configuration
    {
        /// Type of the interpreter. Supported types are "python", "julia" and "R"
        // !! use enum, store interpreter settings in config.xml, parse on load
        std::string interpreter_type;

        // !! add interpreter and lib paths
    };

    explicit InterpretedLangSourceCoordinator(const Configuration & configuration_);

    const Configuration & getConfiguration() const
    {
        return configuration;
    }

    Pipe createPipe(const String& function_body, std::vector<Pipe> && input_pipes, Block sample_block, ContextPtr context);

private:
    Configuration configuration;
    ShellCommandSourceCoordinator cmd_coordinator;
};

}
