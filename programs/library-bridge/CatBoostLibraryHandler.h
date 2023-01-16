#pragma once

#include "CatBoostLibraryAPI.h"

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <base/defines.h>
#include "SharedLibrary.h"

#include <chrono>
#include <mutex>


namespace DB
{

/// Abstracts access to the CatBoost shared library.
class CatBoostLibraryHandler
{
public:
    /// Holds pointers to CatBoost library functions
    struct APIHolder
    {
        explicit APIHolder(SharedLibrary & lib);

        // NOLINTBEGIN(readability-identifier-naming)
        CatBoostLibraryAPI::ModelCalcerCreateFunc ModelCalcerCreate;
        CatBoostLibraryAPI::ModelCalcerDeleteFunc ModelCalcerDelete;
        CatBoostLibraryAPI::GetErrorStringFunc GetErrorString;
        CatBoostLibraryAPI::LoadFullModelFromFileFunc LoadFullModelFromFile;
        CatBoostLibraryAPI::CalcModelPredictionFlatFunc CalcModelPredictionFlat;
        CatBoostLibraryAPI::CalcModelPredictionFunc CalcModelPrediction;
        CatBoostLibraryAPI::CalcModelPredictionWithHashedCatFeaturesFunc CalcModelPredictionWithHashedCatFeatures;
        CatBoostLibraryAPI::GetStringCatFeatureHashFunc GetStringCatFeatureHash;
        CatBoostLibraryAPI::GetIntegerCatFeatureHashFunc GetIntegerCatFeatureHash;
        CatBoostLibraryAPI::GetFloatFeaturesCountFunc GetFloatFeaturesCount;
        CatBoostLibraryAPI::GetCatFeaturesCountFunc GetCatFeaturesCount;
        CatBoostLibraryAPI::GetTreeCountFunc GetTreeCount;
        CatBoostLibraryAPI::GetDimensionsCountFunc GetDimensionsCount;
        // NOLINTEND(readability-identifier-naming)
    };

    CatBoostLibraryHandler(
        const String & library_path,
        const String & model_path);

    ~CatBoostLibraryHandler();

    std::chrono::system_clock::time_point getLoadingStartTime() const;
    std::chrono::milliseconds getLoadingDuration() const;

    size_t getTreeCount() const;

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const;

private:
    std::chrono::system_clock::time_point loading_start_time;
    std::chrono::milliseconds loading_duration;

    const SharedLibraryPtr library;
    const APIHolder api;

    mutable std::mutex mutex;

    CatBoostLibraryAPI::ModelCalcerHandle * model_calcer_handle TSA_GUARDED_BY(mutex) TSA_PT_GUARDED_BY(mutex);

    size_t float_features_count TSA_GUARDED_BY(mutex);
    size_t cat_features_count TSA_GUARDED_BY(mutex);
    size_t tree_count TSA_GUARDED_BY(mutex);

    ColumnFloat64::MutablePtr evalImpl(const ColumnRawPtrs & columns, bool cat_features_are_strings) const TSA_REQUIRES(mutex);
};

using CatBoostLibraryHandlerPtr = std::shared_ptr<CatBoostLibraryHandler>;

}
