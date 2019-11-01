#pragma once

#include <Interpreters/CustomHTTP/HTTPMatchExecutor.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>


namespace DB
{

class HTTPMatchExecutorDefault : public HTTPMatchExecutor
{
protected:

    bool matchImpl(HTTPServerRequest & /*request*/, HTMLForm & /*params*/) const override { return true; }

    bool needParsePostBody(HTTPServerRequest & /*request*/, HTMLForm & /*params*/) const override { return false; }

    String getExecuteQuery(HTMLForm & params) const override
    {
        String execute_query;
        for (const auto & [key, value] : params)
        {
            if (key == "query")
                execute_query += value;
        }

        return execute_query;
    }

    bool acceptQueryParam(Context &context, const String &key, const String &value) const override
    {
        if (startsWith(key, "param_"))
        {
            /// Save name and values of substitution in dictionary.
            context.setQueryParameter(key.substr(strlen("param_")), value);
            return true;
        }

        return key == "query";
    }

};

}
