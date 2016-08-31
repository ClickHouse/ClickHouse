#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/IInterpreter.h>

namespace DB
{

/// Defers the execution of a query. This is mainly used to achieve parallelism
/// when, during the execution of a query on a distributed table, one of the
/// partial queries is executed on the node that has initiated the query.
/// It is especially important if the partial queries use a mechanism in order
/// to synchronize with each other.
class DeferredExecutionBlockInputStream : public IProfilingBlockInputStream
{
public:
	DeferredExecutionBlockInputStream(std::shared_ptr<IInterpreter> interpreter_)
		: interpreter{interpreter_}
	{
	}

	String getName() const override { return "DeferredExecution"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

protected:
	Block readImpl() override
	{
		if (!has_been_executed)
		{
			has_been_executed = true;
			stream = interpreter->execute().in;
		}

		if (stream)
			return stream->read();
		else
			return {};
	}

private:
	std::shared_ptr<IInterpreter> interpreter;
	BlockInputStreamPtr stream;
	bool has_been_executed = false;
};

}

