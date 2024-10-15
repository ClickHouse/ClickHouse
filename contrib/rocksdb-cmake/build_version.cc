// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

#include <memory>

#include "rocksdb/version.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/string_util.h"

// The build script may replace these values with real values based
// on whether or not GIT is available and the platform settings
static const std::string rocksdb_build_git_sha  = "rocksdb_build_git_sha:72438a678872544809393b831c7273794c074215";
static const std::string rocksdb_build_git_tag = "rocksdb_build_git_tag:main";
#define HAS_GIT_CHANGES 0
#if HAS_GIT_CHANGES == 0
// If HAS_GIT_CHANGES is 0, the GIT date is used.
// Use the time the branch/tag was last modified
static const std::string rocksdb_build_date = "rocksdb_build_date:2024-07-12 16:01:57";
#else
// If HAS_GIT_CHANGES is > 0, the branch/tag has modifications.
// Use the time the build was created.
static const std::string rocksdb_build_date = "rocksdb_build_date:2024-07-13 17:15:50";
#endif

extern "C" {

} // extern "C"

std::unordered_map<std::string, ROCKSDB_NAMESPACE::RegistrarFunc> ROCKSDB_NAMESPACE::ObjectRegistry::builtins_ = {
  
};

namespace ROCKSDB_NAMESPACE {
static void AddProperty(std::unordered_map<std::string, std::string> *props, const std::string& name) {
  size_t colon = name.find(":");
  if (colon != std::string::npos && colon > 0 && colon < name.length() - 1) {
    // If we found a "@:", then this property was a build-time substitution that failed.  Skip it
    size_t at = name.find("@", colon);
    if (at != colon + 1) {
      // Everything before the colon is the name, after is the value
      (*props)[name.substr(0, colon)] = name.substr(colon + 1);
    }
  }
}

static std::unordered_map<std::string, std::string>* LoadPropertiesSet() {
  auto * properties = new std::unordered_map<std::string, std::string>();
  AddProperty(properties, rocksdb_build_git_sha);
  AddProperty(properties, rocksdb_build_git_tag);
  AddProperty(properties, rocksdb_build_date);
  return properties;
}

const std::unordered_map<std::string, std::string>& GetRocksBuildProperties() {
  static std::unique_ptr<std::unordered_map<std::string, std::string>> props(LoadPropertiesSet());
  return *props;
}

std::string GetRocksVersionAsString(bool with_patch) {
  std::string version = std::to_string(ROCKSDB_MAJOR) + "." + std::to_string(ROCKSDB_MINOR);
  if (with_patch) {
    return version + "." + std::to_string(ROCKSDB_PATCH);
  } else {
    return version;
 }
}

std::string GetRocksBuildInfoAsString(const std::string& program, bool verbose) {
  std::string info = program + " (RocksDB) " + GetRocksVersionAsString(true);
  if (verbose) {
    for (const auto& it : GetRocksBuildProperties()) {
      info.append("\n    ");
      info.append(it.first);
      info.append(": ");
      info.append(it.second);
    }
  }
  return info;
}
} // namespace ROCKSDB_NAMESPACE
