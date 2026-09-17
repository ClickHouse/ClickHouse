#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>

#include <memory>

namespace DB
{

class Context;

namespace UDT
{
class UDTExecutionBoundaryProof;
class UDTStoredObjectDDLSelectBoundaryHandoff;
}

class InterpreterFactory : private boost::noncopyable
{
public:
    static InterpreterFactory & instance();

    struct Arguments
    {
        ASTPtr & query;
        ContextMutablePtr context;
        const SelectQueryOptions & options;
        bool allow_materialized = false;
        std::shared_ptr<UDT::UDTStoredObjectDDLSelectBoundaryHandoff> udt_stored_object_ddl_select_boundary_handoff;
    };

    using InterpreterPtr = std::unique_ptr<IInterpreter>;

    InterpreterPtr
    get(ASTPtr & query,
        ContextMutablePtr context,
        UDT::UDTExecutionBoundaryProof udt_execution_boundary,
        const SelectQueryOptions & options = {});

    using CreatorFn = std::function<InterpreterPtr(const Arguments & arguments)>;

    using Interpreters = std::unordered_map<String, CreatorFn>;

    void registerInterpreter(const std::string & name, CreatorFn creator_fn);

private:
    Interpreters interpreters;
};

}
