#pragma once

#include <BridgeHelper/LibraryBridgeHelper.h>
#include <Common/ExternalModelInfo.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <optional>

namespace DB
{

class CatBoostLibraryBridgeHelper : public LibraryBridgeHelper
{
public:
    static constexpr inline auto PING_HANDLER = "/catboost_ping";
    static constexpr inline auto MAIN_HANDLER = "/catboost_request";

    explicit CatBoostLibraryBridgeHelper(
        ContextPtr context_,
        std::optional<String> model_path_ = std::nullopt,
        std::optional<String> library_path_ = std::nullopt);

    ExternalModelInfos listModels();

    void removeModel();                                            /// requires model_path
    void removeAllModels();

    size_t getTreeCount();                                         /// requires model_path and library_path
    ColumnPtr evaluate(const ColumnsWithTypeAndName & columns);    /// requires model_path

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

    const std::optional<String> model_path;
    const std::optional<String> library_path;
};

}
