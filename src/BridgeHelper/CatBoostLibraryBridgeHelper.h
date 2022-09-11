#pragma once

#include <BridgeHelper/LibraryBridgeHelper.h>
#include <Common/ExternalModelInfo.h>
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

    CatBoostLibraryBridgeHelper(ContextPtr context_, std::string_view library_path_, std::string_view model_path_);
    CatBoostLibraryBridgeHelper(ContextPtr context_, std::string_view model_path_);
    explicit CatBoostLibraryBridgeHelper(ContextPtr context_);

    ExternalModelInfos listModels();

    void removeModel();
    void removeAllModels();

    size_t getTreeCount();
    ColumnPtr evaluate(const ColumnsWithTypeAndName & columns);

protected:
    Poco::URI getPingURI() const override;

    Poco::URI getMainURI() const override;

    bool bridgeHandShake() override;

private:
    static constexpr inline auto CATBOOST_LIST_METHOD = "catboost_list";
    static constexpr inline auto CATBOOST_REMOVEMODEL_METHOD = "catboost_removeModel";
    static constexpr inline auto CATBOOST_REMOVEALLMODELS_METHOD = "catboost_removeAllModels";
    static constexpr inline auto CATBOOST_GETTREECOUNT_METHOD = "catboost_GetTreeCount";
    static constexpr inline auto CATBOOST_LIB_EVALUATE_METHOD = "catboost_libEvaluate";

    Poco::URI createRequestURI(const String & method) const;

    const String library_path;
    const String model_path;
};

}
