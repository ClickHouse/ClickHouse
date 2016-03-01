#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/IInterpreter.h>
#include <DB/Interpreters/ClusterProxy/PreSendHook.h>
#include <Poco/SharedPtr.h>
#include <functional>

namespace DB
{

class PreSendCallbackInputStream : public IProfilingBlockInputStream
{
public:
	PreSendCallbackInputStream(Poco::SharedPtr<IInterpreter> & interpreter_, ClusterProxy::PreSendHook::Callback callback)
		: interpreter(interpreter_), pre_send_callback(std::bind(callback, nullptr))
	{
	}

	String getName() const override { return "PreSendCallbackInput"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

protected:
	Block readImpl() override
	{
		if (!is_sent)
		{
			is_sent = true;
			pre_send_callback();
			stream = interpreter->execute().in;
		}
		return stream->read();
	}

private:
	bool is_sent = false;
	Poco::SharedPtr<IInterpreter> interpreter;
	BlockInputStreamPtr stream;
	std::function<void()> pre_send_callback;
};

}
