#pragma once

#include <vector>
#include <cmath>
#include <limits>

constexpr int CHUNK_SIZE = 128;

class Store {
public:
    virtual ~Store() = default;
    double count = 0;
    int min_key = std::numeric_limits<int>::max();
    int max_key = std::numeric_limits<int>::min();
    int offset = 0;
    std::vector<double> bins;

    virtual void copy(Store* store) = 0;
    virtual int length() = 0;
    virtual void add(int key, double weight = 1.0) = 0;
    virtual int keyAtRank(double rank, bool lower = true) = 0;
    virtual void merge(Store* store) = 0;
};

class DenseStore : public Store {
public:
    DenseStore(int chunk_size = CHUNK_SIZE) : chunk_size(chunk_size) {}

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

    void add(int key, double weight = 1.0) override {
        int idx = getIndex(key);
        bins[idx] += weight;
        count += weight;
    }

    int keyAtRank(double rank, bool lower = true) override {
        double running_ct = 0.0;
        for (int i = 0; i < bins.size(); ++i) {
            running_ct += bins[i];
            if ((lower && running_ct > rank) || (!lower && running_ct >= rank + 1)) {
                return i + offset;
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
        return chunk_size * std::ceil(static_cast<double>(desired_length) / chunk_size);
    }

    void extendRange(int key, int second_key) {
        int new_min_key = std::min({key, min_key});
        int new_max_key = std::max({second_key, max_key});

        if (length() == 0) {
            bins = std::vector<double>(getNewLength(new_min_key, new_max_key), 0.0);
            offset = new_min_key;
            adjust(new_min_key, new_max_key);
        } else if (new_min_key >= min_key && new_max_key < offset + length()) {
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