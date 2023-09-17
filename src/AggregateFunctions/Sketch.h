#pragma once

#include <memory> // for std::unique_ptr
#include <cmath>
#include <stdexcept>
#include <limits>
#include <iostream>
#include <base/types.h>

#include "Mapping.h"
#include "Store.h"

namespace DB {

class BaseQuantileSketch {
public:
    BaseQuantileSketch(std::unique_ptr<KeyMapping> mapping_, std::unique_ptr<DenseStore> store_,
                       std::unique_ptr<DenseStore> negative_store_, Float64 zero_count_)
        : mapping(std::move(mapping_)), store(std::move(store_)), negative_store(std::move(negative_store_)),
          zero_count(zero_count_),
          count(static_cast<Float64>(negative_store->count + zero_count_ + store->count)),
          min(std::numeric_limits<Float64>::infinity()), max(-std::numeric_limits<Float64>::infinity()), sum(0.0) {}

    void add(Float64 val, Float64 weight = 1.0) {
        if (weight <= 0.0) {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "weight must be a positive Float64");
        }

        if (val > mapping->getMinPossible()) {
            store->add(mapping->key(val), weight);
        } else if (val < -mapping->getMinPossible()) {
            negative_store->add(mapping->key(-val), weight);
        } else {
            zero_count += weight;
        }

        count += weight;
        sum += val * weight;
        min = std::min(min, val);
        max = std::max(max, val);
    }

    Float64 get(Float64 quantile) const {
        if (quantile < 0 || quantile > 1 || count == 0) {
            return std::numeric_limits<Float64>::quiet_NaN(); // Return NaN if the conditions are not met
        }

        Float64 rank = quantile * (count - 1);
        Float64 quantile_value;
        if (rank < negative_store->count) {
            Float64 reversed_rank = negative_store->count - rank - 1;
            int key = negative_store->keyAtRank(reversed_rank, false);
            quantile_value = -mapping->value(key);
        } else if (rank < zero_count + negative_store->count) {
            quantile_value = 0;
        } else {
            int key = store->keyAtRank(rank - zero_count - negative_store->count);
            quantile_value = mapping->value(key);
        }
        return quantile_value;
    }

    void merge(BaseQuantileSketch& sketch) {
        if (!mergeable(sketch)) {
            throw std::invalid_argument("Cannot merge two QuantileSketches with different parameters");
        }

        if (sketch.count == 0) {
            return;
        }

        if (count == 0) {
            copy(sketch);
            return;
        }

        store->merge(sketch.store.get());
        negative_store->merge(sketch.negative_store.get());
        zero_count += sketch.zero_count;

        count += sketch.count;
        sum += sketch.sum;
        min = std::min(min, sketch.min);
        max = std::max(max, sketch.max);
    }

    bool mergeable(const BaseQuantileSketch& other) {
        return mapping->getGamma() == other.mapping->getGamma();
    }

    void copy(const BaseQuantileSketch& sketch) {
        store->copy(sketch.store.get());
        negative_store->copy(sketch.negative_store.get());
        zero_count = sketch.zero_count;
        min = sketch.min;
        max = sketch.max;
        count = sketch.count;
        sum = sketch.sum;
    }

private:
    std::unique_ptr<KeyMapping> mapping;
    std::unique_ptr<DenseStore> store;
    std::unique_ptr<DenseStore> negative_store;
    Float64 zero_count;
    Float64 count;
    Float64 min;
    Float64 max;
    Float64 sum;
};

class Sketch : public BaseQuantileSketch {
public:
    Sketch(Float64 relative_accuracy = 0.01)
        : BaseQuantileSketch(std::make_unique<LogarithmicMapping>(relative_accuracy),
                             std::make_unique<DenseStore>(),
                             std::make_unique<DenseStore>(), 0.0) {}
};

}
