#pragma once

#include <memory>
#include <optional>
#include <string>

#include <Interpreters/Context_fwd.h>
#include <Server/HTTPHandler.h>
#include <Poco/Logger.h>

namespace DB
{

class TabularHandler : public HTTPHandler
{
public:
    TabularHandler(IServer & server_, const std::optional<String> & content_type_override_ = std::nullopt);

    std::shared_ptr<QueryData> getQueryAST(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) override;

    bool customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value) override;
};

}
