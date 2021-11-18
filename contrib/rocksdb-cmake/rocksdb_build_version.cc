// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
/// This file was edited for ClickHouse.

#include <memory>

#include "rocksdb/version.h"
#include "util/string_util.h"

// The build script may replace these values with real values based
// on whether or not GIT is available and the platform settings
static const std::string rocksdb_build_git_sha  = "rocksdb_build_git_sha:0";
static const std::string rocksdb_build_git_tag = "rocksdb_build_git_tag:master";
static const std::string rocksdb_build_date = "rocksdb_build_date:2000-01-01";

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
  std::string version = ToString(ROCKSDB_MAJOR) + "." + ToString(ROCKSDB_MINOR);
  if (with_patch) {
    return version + "." + ToString(ROCKSDB_PATCH);
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
