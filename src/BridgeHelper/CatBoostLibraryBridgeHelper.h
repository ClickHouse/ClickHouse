#pragma once

#include <BridgeHelper/LibraryBridgeHelper.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>


namespace DB
{

class CatBoostLibraryBridgeHelper : public LibraryBridgeHelper
{
public:
    static constexpr inline auto PING_HANDLER = "/catboost_ping";
    static constexpr inline auto MAIN_HANDLER = "/catboost_request";

    CatBoostLibraryBridgeHelper(ContextPtr context_, const String & library_path_, const String & model_path_);

    size_t getTreeCount();

    ColumnPtr evaluate(const ColumnsWithTypeAndName & columns);

protected:
    Poco::URI getPingURI() const override;

    Poco::URI getMainURI() const override;

    bool bridgeHandShake() override;

private:
    static constexpr inline auto CATBOOST_GETTREECOUNT_METHOD = "catboost_GetTreeCount";
    static constexpr inline auto CATBOOST_LIB_EVALUATE_METHOD = "catboost_libEvaluate";

    Poco::URI createRequestURI(const String & method) const;

    const String library_path;
    const String model_path;
};

}
