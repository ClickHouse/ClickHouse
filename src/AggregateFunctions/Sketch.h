#pragma once

#include <cmath>
#include <stdexcept>
#include <string>
#include <limits>
#include <iostream>

#include "Mapping.h"
#include "Store.h"

class BaseQuantileSketch {
public:
    BaseQuantileSketch(KeyMapping* mapping, DenseStore* store, DenseStore* negative_store, float zero_count)
        : mapping(mapping), store(store), negative_store(negative_store), zero_count(zero_count),
          relative_accuracy(0.01), count(negative_store->count + zero_count + store->count),
          min(std::numeric_limits<float>::infinity()), max(-std::numeric_limits<float>::infinity()), sum(0.0) {}

    void add(float val, float weight = 1.0) {
        if (weight <= 0.0) {
            throw std::invalid_argument("weight must be a positive float");
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

    float get(float quantile) {
        if (quantile < 0 || quantile > 1 || count == 0) {
            return std::numeric_limits<float>::quiet_NaN(); // Return NaN if the conditions are not met
        }

        float rank = quantile * (count - 1);
        float quantile_value;
        if (rank < negative_store->count) {
            float reversed_rank = negative_store->count - rank - 1;
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

        store->merge(sketch.store);
        negative_store->merge(sketch.negative_store);
        zero_count += sketch.zero_count;

        count += sketch.count;
        sum += sketch.sum;
        min = std::min(min, sketch.min);
        max = std::max(max, sketch.max);
    }

private:
    bool mergeable(const BaseQuantileSketch& other) {
        return mapping->getGamma() == other.mapping->getGamma();
    }

    void copy(const BaseQuantileSketch& sketch) {
        store->copy(sketch.store);
        negative_store->copy(sketch.negative_store);
        zero_count = sketch.zero_count;
        min = sketch.min;
        max = sketch.max;
        count = sketch.count;
        sum = sketch.sum;
    }

private:
    KeyMapping* mapping;
    DenseStore* store;
    DenseStore* negative_store;
    float zero_count;
    float relative_accuracy;
    float count;
    float min;
    float max;
    float sum;
};

class QuantileSketch : public BaseQuantileSketch {
public:
    QuantileSketch(float relative_accuracy = 0.01) 
        : BaseQuantileSketch(new LogarithmicMapping(relative_accuracy), new DenseStore(), new DenseStore(), 0.0) {}
};