#pragma once

#include <base/types.h>
#include <vector>
#include <cmath>
#include <limits>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


// We start with 128 bins and grow the number of bins by 128 each time we need to extend the range
// of the bins. This is done to avoid reallocating the bins vector too often.
constexpr int CHUNK_SIZE = 128;

namespace DB {

class Store {
public:
    virtual ~Store() = default;
    Float64 count = 0;
    int min_key = std::numeric_limits<int>::max();
    int max_key = std::numeric_limits<int>::min();
    int offset = 0;
    std::vector<Float64> bins;

    virtual void copy(Store* store) = 0;
    virtual int length() = 0;
    virtual void add(int key, Float64 weight = 1.0) = 0;
    virtual int keyAtRank(Float64 rank, bool lower = true) = 0;
    virtual void merge(Store* store) = 0;
};

class DenseStore : public Store {
public:
    DenseStore(int chunk_size_ = CHUNK_SIZE) : chunk_size(chunk_size_) {}

    void copy(Store* other) override {
        bins = other->bins;
        count = other->count;
        min_key = other->min_key;
        max_key = other->max_key;
        offset = other->offset;
    }

    int length() override {
        return static_cast<int>(bins.size());
    }

    void add(int key, Float64 weight = 1.0) override {
        int idx = getIndex(key);
        bins[idx] += weight;
        count += weight;
    }

    int keyAtRank(Float64 rank, bool lower = true) override {
        Float64 running_ct = 0.0;
        for (size_t i = 0; i < bins.size(); ++i) {  // Fixed sign comparison
            running_ct += bins[i];
            if ((lower && running_ct > rank) || (!lower && running_ct >= rank + 1)) {
                return static_cast<int>(i) + offset;
            }
        }
        return max_key;
    }

    void merge(Store* other) override {
        if (other->count == 0) return;

        if (count == 0) {
            copy(other);
            return;
        }

        if (other->min_key < min_key || other->max_key > max_key) {
            extendRange(other->min_key, other->max_key);
        }

        for (int key = other->min_key; key <= other->max_key; ++key) {
            bins[key - offset] += other->bins[key - other->offset];
        }

        count += other->count;
    }

    void serialize(WriteBuffer& buf) const {
        writeBinary(min_key, buf);
        writeBinary(max_key, buf);
        writeBinary(offset, buf);
        writeBinary(count, buf);
        writeBinary(bins, buf);
    }

    void deserialize(ReadBuffer& buf) {
        readBinary(min_key, buf);
        readBinary(max_key, buf);
        readBinary(offset, buf);
        readBinary(count, buf);
        readBinary(bins, buf);
    }

private:
    int chunk_size;

    int getIndex(int key) {
        if (key < min_key || key > max_key) {
            extendRange(key, key);
        }
        return key - offset;
    }

    int getNewLength(int new_min_key, int new_max_key) {
        int desired_length = new_max_key - new_min_key + 1;
        return static_cast<int>(chunk_size * std::ceil(static_cast<Float64>(desired_length) / chunk_size)); // Fixed float conversion
    }

    void extendRange(int key, int second_key) {
        int new_min_key = std::min({key, min_key});
        int new_max_key = std::max({second_key, max_key});

        if (length() == 0) {
            bins = std::vector<Float64>(getNewLength(new_min_key, new_max_key), 0.0);
            offset = new_min_key;
            adjust(new_min_key, new_max_key);
        } else if (new_min_key >= offset && new_max_key < offset + length()) {
            min_key = new_min_key;
            max_key = new_max_key;
        } else {
            int new_length = getNewLength(new_min_key, new_max_key);
            bins.resize(new_length, 0.0);
            adjust(new_min_key, new_max_key);
        }
    }

    void adjust(int new_min_key, int new_max_key) {
        centerBins(new_min_key, new_max_key);
        min_key = new_min_key;
        max_key = new_max_key;
    }

    void shiftBins(int shift) {
        if (shift > 0) {
            bins.insert(bins.begin(), shift, 0.0);
            bins.resize(bins.size() - shift);
        } else {
            bins.erase(bins.begin(), bins.begin() + std::abs(shift));
            bins.resize(bins.size() + std::abs(shift), 0.0);
        }
        offset -= shift;
    }

    void centerBins(int new_min_key, int new_max_key) {
        int middle_key = new_min_key + (new_max_key - new_min_key + 1) / 2;
        shiftBins(offset + length() / 2 - middle_key);
    }
};

}
