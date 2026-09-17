#include <Interpreters/UDT/StoredObjectTableFunctionSources.h>

#include <Common/StringUtils.h>

#include <array>

namespace DB::UDT
{
namespace
{

using Contract = StoredObjectTableFunctionSourceContract;
using Provenance = StoredObjectTableFunctionSourceProvenance;

/// This inventory is deliberately explicit. Query/object-backed functions
/// whose output can inherit a stored ClickHouse schema require exact logical
/// authority; external/fixed-schema functions are explicitly physical. An
/// absent name is Unclassified and fails closed at UDT-enabled inferred CREATE.
constexpr std::array contracts{
    Contract{"SQLStandardValues", Provenance::PhysicalInference},
    Contract{"arrowFlight", Provenance::PhysicalInference},
    Contract{"arrowflight", Provenance::PhysicalInference},
    Contract{"azureBlobStorage", Provenance::PhysicalInference},
    Contract{"azureBlobStorageCluster", Provenance::PhysicalInference},
    Contract{"bigquery", Provenance::PhysicalInference},
    Contract{"cluster", Provenance::PhysicalInference},
    Contract{"clusterAllReplicas", Provenance::PhysicalInference},
    Contract{"cosn", Provenance::PhysicalInference},
    Contract{"deltaLake", Provenance::PhysicalInference},
    Contract{"deltaLakeAzure", Provenance::PhysicalInference},
    Contract{"deltaLakeAzureCluster", Provenance::PhysicalInference},
    Contract{"deltaLakeCluster", Provenance::PhysicalInference},
    Contract{"deltaLakeLocal", Provenance::PhysicalInference},
    Contract{"deltaLakeS3", Provenance::PhysicalInference},
    Contract{"deltaLakeS3Cluster", Provenance::PhysicalInference},
    Contract{"dictionary", Provenance::ExactLogicalAuthorityRequired},
    /// eval() parses an arbitrary query string and can expose a mapped local
    /// object's schema; it is never safe to infer a durable physical schema.
    Contract{"eval", Provenance::ExactLogicalAuthorityRequired},
    Contract{"executable", Provenance::PhysicalInference},
    Contract{"file", Provenance::PhysicalInference},
    Contract{"fileCluster", Provenance::PhysicalInference},
    Contract{"filesystem", Provenance::PhysicalInference, true},
    Contract{"format", Provenance::PhysicalInference, true},
    Contract{"fuzzJSON", Provenance::PhysicalInference},
    Contract{"fuzzQuery", Provenance::PhysicalInference},
    Contract{"gcs", Provenance::PhysicalInference},
    Contract{"generateRandom", Provenance::PhysicalInference},
    Contract{"generateSeries", Provenance::PhysicalInference},
    Contract{"generate_series", Provenance::PhysicalInference},
    Contract{"hdfs", Provenance::PhysicalInference},
    Contract{"hdfsCluster", Provenance::PhysicalInference},
    Contract{"hive", Provenance::PhysicalInference},
    Contract{"hudi", Provenance::PhysicalInference},
    Contract{"hudiCluster", Provenance::PhysicalInference},
    Contract{"iceberg", Provenance::PhysicalInference},
    Contract{"icebergAzure", Provenance::PhysicalInference},
    Contract{"icebergAzureCluster", Provenance::PhysicalInference},
    Contract{"icebergCluster", Provenance::PhysicalInference},
    Contract{"icebergHDFS", Provenance::PhysicalInference},
    Contract{"icebergHDFSCluster", Provenance::PhysicalInference},
    Contract{"icebergLocal", Provenance::PhysicalInference},
    Contract{"icebergLocalCluster", Provenance::PhysicalInference},
    Contract{"icebergS3", Provenance::PhysicalInference},
    Contract{"icebergS3Cluster", Provenance::PhysicalInference},
    Contract{"input", Provenance::PhysicalInference},
    Contract{"jdbc", Provenance::PhysicalInference},
    Contract{"loop", Provenance::ExactLogicalAuthorityRequired},
    Contract{"merge", Provenance::ExactLogicalAuthorityRequired},
    Contract{"mergeTreeAnalyzeIndexes", Provenance::PhysicalInference},
    Contract{"mergeTreeAnalyzeIndexesUUID", Provenance::PhysicalInference},
    Contract{"mergeTreeCodecBlockCounts", Provenance::PhysicalInference},
    Contract{"mergeTreeIndex", Provenance::ExactLogicalAuthorityRequired},
    Contract{"mergeTreeProjection", Provenance::ExactLogicalAuthorityRequired},
    Contract{"mergeTreeTextIndex", Provenance::PhysicalInference},
    Contract{"mongodb", Provenance::PhysicalInference},
    Contract{"mysql", Provenance::PhysicalInference},
    Contract{"null", Provenance::PhysicalInference},
    Contract{"numbers", Provenance::PhysicalInference},
    Contract{"numbers_mt", Provenance::PhysicalInference},
    Contract{"odbc", Provenance::PhysicalInference},
    Contract{"oss", Provenance::PhysicalInference},
    Contract{"paimon", Provenance::PhysicalInference},
    Contract{"paimonAzure", Provenance::PhysicalInference},
    Contract{"paimonAzureCluster", Provenance::PhysicalInference},
    Contract{"paimonCluster", Provenance::PhysicalInference},
    Contract{"paimonHDFS", Provenance::PhysicalInference},
    Contract{"paimonHDFSCluster", Provenance::PhysicalInference},
    Contract{"paimonLocal", Provenance::PhysicalInference},
    Contract{"paimonS3", Provenance::PhysicalInference},
    Contract{"paimonS3Cluster", Provenance::PhysicalInference},
    Contract{"postgresql", Provenance::PhysicalInference},
    Contract{"primes", Provenance::PhysicalInference},
    Contract{"prometheusQuery", Provenance::PhysicalInference},
    Contract{"prometheusQueryRange", Provenance::PhysicalInference},
    Contract{"redis", Provenance::PhysicalInference},
    Contract{"remote", Provenance::PhysicalInference},
    Contract{"remoteSecure", Provenance::PhysicalInference},
    Contract{"s3", Provenance::PhysicalInference},
    Contract{"s3Cluster", Provenance::PhysicalInference},
    Contract{"sqlite", Provenance::PhysicalInference},
    Contract{"timeSeriesData", Provenance::ExactLogicalAuthorityRequired},
    Contract{"timeSeriesMetrics", Provenance::ExactLogicalAuthorityRequired},
    Contract{"timeSeriesSamples", Provenance::ExactLogicalAuthorityRequired},
    Contract{"timeSeriesSelector", Provenance::PhysicalInference},
    Contract{"timeSeriesTags", Provenance::ExactLogicalAuthorityRequired},
    Contract{"url", Provenance::PhysicalInference},
    Contract{"urlCluster", Provenance::PhysicalInference},
    Contract{"values", Provenance::PhysicalInference, true},
    Contract{"view", Provenance::ExactLogicalAuthorityRequired},
    Contract{"viewExplain", Provenance::PhysicalInference},
    Contract{"viewIfPermitted", Provenance::ExactLogicalAuthorityRequired},
    Contract{"ytsaurus", Provenance::PhysicalInference},
    Contract{"zeros", Provenance::PhysicalInference},
    Contract{"zeros_mt", Provenance::PhysicalInference},
};

constexpr char asciiLower(char value) noexcept
{
    return value >= 'A' && value <= 'Z' ? static_cast<char>(value - 'A' + 'a') : value;
}

constexpr bool equalCaseInsensitive(std::string_view lhs, std::string_view rhs) noexcept
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t index = 0; index < lhs.size(); ++index)
        if (asciiLower(lhs[index]) != asciiLower(rhs[index]))
            return false;
    return true;
}

constexpr bool contractsAreClosed() noexcept
{
    for (size_t index = 0; index < contracts.size(); ++index)
    {
        if (contracts[index].function_name.empty())
            return false;
        if (index && !(contracts[index - 1].function_name < contracts[index].function_name))
            return false;
        for (size_t previous = 0; previous < index; ++previous)
            if ((contracts[index].case_insensitive || contracts[previous].case_insensitive)
                && equalCaseInsensitive(contracts[index].function_name, contracts[previous].function_name))
                return false;
    }
    return true;
}

static_assert(contractsAreClosed());

}

std::span<const StoredObjectTableFunctionSourceContract> getStoredObjectTableFunctionSourceContracts() noexcept
{
    return contracts;
}

const StoredObjectTableFunctionSourceContract * tryGetStoredObjectTableFunctionSourceContract(std::string_view function_name) noexcept
{
    for (const auto & contract : contracts)
        if (contract.function_name == function_name
            || (contract.case_insensitive && equalsCaseInsensitive(contract.function_name, function_name)))
            return &contract;
    return nullptr;
}

bool storedObjectTableFunctionRequiresExactLogicalAuthority(std::string_view function_name) noexcept
{
    return classifyStoredObjectTableFunctionSource(function_name)
        == StoredObjectTableFunctionSourceProvenance::ExactLogicalAuthorityRequired;
}

StoredObjectTableFunctionSourceProvenance classifyStoredObjectTableFunctionSource(std::string_view function_name) noexcept
{
    const auto * contract = tryGetStoredObjectTableFunctionSourceContract(function_name);
    return contract ? contract->provenance : StoredObjectTableFunctionSourceProvenance::Unclassified;
}

}
