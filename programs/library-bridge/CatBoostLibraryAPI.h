#pragma once

#include <cstdint>
#include <cstddef>

// Function pointer typedefs and names of libcatboost.so functions used by ClickHouse
struct CatBoostLibraryAPI
{
    using ModelCalcerHandle = void;

    using ModelCalcerCreateFunc = ModelCalcerHandle * (*)();
    static constexpr const char * ModelCalcerCreateName = "ModelCalcerCreate";

    using ModelCalcerDeleteFunc = void (*)(ModelCalcerHandle *);
    static constexpr const char * ModelCalcerDeleteName = "ModelCalcerDelete";

    using GetErrorStringFunc = const char * (*)();
    static constexpr const char * GetErrorStringName = "GetErrorString";

    using LoadFullModelFromFileFunc = bool (*)(ModelCalcerHandle *, const char *);
    static constexpr const char * LoadFullModelFromFileName = "LoadFullModelFromFile";

    using CalcModelPredictionFlatFunc = bool (*)(ModelCalcerHandle *, size_t, const float **, size_t, double *, size_t);
    static constexpr const char * CalcModelPredictionFlatName = "CalcModelPredictionFlat";

    using CalcModelPredictionFunc = bool (*)(ModelCalcerHandle *, size_t, const float **, size_t, const char ***, size_t, double *, size_t);
    static constexpr const char * CalcModelPredictionName = "CalcModelPrediction";

    using CalcModelPredictionWithHashedCatFeaturesFunc = bool (*)(ModelCalcerHandle *, size_t, const float **, size_t, const int **, size_t, double *, size_t);
    static constexpr const char * CalcModelPredictionWithHashedCatFeaturesName = "CalcModelPredictionWithHashedCatFeatures";

    using GetStringCatFeatureHashFunc = int (*)(const char *, size_t);
    static constexpr const char * GetStringCatFeatureHashName = "GetStringCatFeatureHash";

    using GetIntegerCatFeatureHashFunc = int (*)(uint64_t);
    static constexpr const char * GetIntegerCatFeatureHashName = "GetIntegerCatFeatureHash";

    using GetFloatFeaturesCountFunc = size_t (*)(ModelCalcerHandle *);
    static constexpr const char * GetFloatFeaturesCountName = "GetFloatFeaturesCount";

    using GetCatFeaturesCountFunc = size_t (*)(ModelCalcerHandle *);
    static constexpr const char * GetCatFeaturesCountName = "GetCatFeaturesCount";

    using GetTreeCountFunc = size_t (*)(ModelCalcerHandle *);
    static constexpr const char * GetTreeCountName = "GetTreeCount";

    using GetDimensionsCountFunc = size_t (*)(ModelCalcerHandle *);
    static constexpr const char * GetDimensionsCountName = "GetDimensionsCount";
};
