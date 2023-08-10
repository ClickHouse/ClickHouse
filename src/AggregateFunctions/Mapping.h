#pragma once

#include <cmath>
#include <stdexcept>
#include <limits>

class KeyMapping {
public:
    KeyMapping(double relative_accuracy, double offset = 0.0)
        : relative_accuracy(relative_accuracy), offset(offset) {
        
        if (relative_accuracy <= 0 || relative_accuracy >= 1) {
            throw std::invalid_argument("Relative accuracy must be between 0 and 1.");
        }

        double gamma_mantissa = 2 * relative_accuracy / (1 - relative_accuracy);
        gamma = 1 + gamma_mantissa;
        multiplier = 1 / std::log1p(gamma_mantissa);
        min_possible = std::numeric_limits<double>::min() * gamma;
        max_possible = std::numeric_limits<double>::max() / gamma;
    }

    virtual double logGamma(double value) const = 0;
    virtual double powGamma(double value) const = 0;

    int key(double value) const {
        return static_cast<int>(std::ceil(logGamma(value)) + offset);
    }

    double value(int key) const {
        return powGamma(key - offset) * (2.0 / (1 + gamma));
    }

    double getGamma() const {
        return gamma;
    }

    double getMinPossible() const {
        return min_possible;
    }

protected:
    double relative_accuracy;
    double gamma;
    double min_possible;
    double max_possible;
    double multiplier;
    double offset;
};

class LogarithmicMapping : public KeyMapping {
public:
    LogarithmicMapping(double relative_accuracy, double offset = 0.0)
        : KeyMapping(relative_accuracy, offset) {
        multiplier *= std::log(2);
    }

    double logGamma(double value) const {
        return std::log(value) / std::log(2.0) * multiplier;
    }

    double powGamma(double value) const {
        return std::pow(2.0, value / multiplier);
    }
};