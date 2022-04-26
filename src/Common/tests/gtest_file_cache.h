#pragma once

#include <filesystem>
#include <iomanip>
#include <iostream>
#include <thread>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <gtest/gtest.h>
#include <Common/ARCFileCache.h>
#include <Common/CurrentThread.h>
#include <Common/FileCacheSettings.h>
#include <Common/SipHash.h>
#include <Common/filesystemHelpers.h>
#include <Common/hex.h>
#include <Common/tests/gtest_global_context.h>

namespace fs = std::filesystem;

extern fs::path caches_dir;
extern String cache_base_path;

void assertRange(
    [[maybe_unused]] size_t assert_n,
    DB::FileSegmentPtr file_segment,
    const DB::FileSegment::Range & expected_range,
    DB::FileSegment::State expected_state);

void printRanges(std::vector<DB::FileSegmentPtr> & segments);

std::vector<DB::FileSegmentPtr> fromHolder(const DB::FileSegmentsHolder & holder);

String keyToStr(const DB::IFileCache::Key & key);

String getFileSegmentPath(const String & base_path, const DB::IFileCache::Key & key, size_t offset);

void download(DB::FileSegmentPtr file_segment);

void prepareAndDownload(DB::FileSegmentPtr file_segment);

void complete(const DB::FileSegmentsHolder & holder);
