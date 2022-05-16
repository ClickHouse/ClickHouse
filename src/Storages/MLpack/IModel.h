#pragma once

// #include <Disks/IDisk.h>
// #include <Poco/Util/Application.h>
// #include <IO/WriteBufferFromFileDescriptor.h>
// #include <IO/ReadBufferFromFileDescriptor.h>
// #include <IO/copyData.h>
// #include <boost/program_options.hpp>
// #include <Common/TerminalSize.h>
#include <Storages/MLpack/ModelSettings.h>
#include <mlpack/methods/linear_regression/linear_regression.hpp>
#include <mlpack/methods/logistic_regression/logistic_regression.hpp>
#include <mlpack/methods/linear_svm/linear_svm.hpp>
#include <mlpack/core.hpp>
#include <ensmallen.hpp>
#include <Common/PODArray.h>
#include <Common/logger_useful.h>

// namespace ErrorCodes
// {
// extern const int CANNOT_SAVE_MLPACK_MODEL;
// }

namespace DB
{

struct TrainResult {};

struct LinearRegressionResult : public TrainResult
{
   const mlpack::regression::LinearRegression& model;
};

struct LogisticRegressionResult : public TrainResult
{
   const mlpack::regression::LogisticRegression<>& model;
};

struct LinearSVMResult : public TrainResult
{
   const mlpack::svm::LinearSVM<>& model;
};


class IModel
{
public:
    IModel() = default;
    virtual ~IModel() = default;

    virtual String GetName() const = 0;

    virtual TrainResult GetModel() const = 0;

    virtual void TrainArb(const arma::mat& regressors, Float64 * target_ptr, size_t n_rows) = 0;
    
    virtual bool Save(String filepath) = 0;

};

class LinReg: public IModel 
{
public:
    LinReg(
        std::unique_ptr<ModelSettings> model_settings)
        : IModel()
    {
        model.Lambda() = model_settings->lambda.value;
    } 

    String GetName() const override { return "LinearRegression"; }

    TrainResult GetModel() const override { return LinearRegressionResult{{}, model}; }

    void TrainArb(const arma::mat& regressors, Float64 * target_ptr, size_t n_rows) override
    {
        arma::rowvec target = arma::rowvec(target_ptr, n_rows);
        model.Train(regressors, target);
    }

    bool Save(String filepath) override
    {
        return mlpack::data::Save(filepath, "linreg", model);
    }

protected:
    mlpack::regression::LinearRegression model;    
};

class LogReg: public IModel 
{
public:
    LogReg(
        std::unique_ptr<ModelSettings> model_settings)
        : IModel()
    {
        model.Lambda() = model_settings->lambda.value;
        // auto opt_type = model_settings->optimizer.value;
    } 

    String GetName() const override { return "LogisticRegression"; }

    TrainResult GetModel() const override { return LogisticRegressionResult{{}, model}; }

    // void Train(const arma::mat& regressors, const arma::Row<double>& target) override {
    //     ens::L_BFGS lbfgsOpt;

    //     model.Train(regressors, target, lbfgsOpt);
    // }

    void TrainArb(const arma::mat& regressors, Float64 * target_ptr, size_t n_rows) override {
        PODArray<size_t> modified_target(n_rows);
        for (size_t i = 0; i < n_rows; ++i)
        {
            modified_target[i] = static_cast<size_t>(target_ptr[i]);
        }
        // size_t * modified_ptr = reinterpret_cast<size_t *>(target_ptr); 
        arma::Row<size_t> target = arma::Row<size_t>(modified_target.data(), n_rows);
        model.Train(regressors, target);
    }

    bool Save(String filepath) override
    {
        return mlpack::data::Save(filepath, "logreg", model);
    }

protected:
    mlpack::regression::LogisticRegression<> model;

};

class LinearSVM: public IModel 
{
public:
    LinearSVM(
        std::unique_ptr<ModelSettings> model_settings)
        : IModel()
    {
        model.Lambda() = model_settings->lambda.value;
        model.Delta() = model_settings->delta.value;
    } 

    String GetName() const override { return "LinearSVM"; }

    TrainResult GetModel() const override { return LinearSVMResult{{}, model}; }

    // void Train(const arma::mat& regressors, const arma::Row<double>& target) override {
    //     ens::L_BFGS lbfgsOpt;
    //     model.Train(regressors, target, 2, lbfgsOpt);
    // }

    void TrainArb(const arma::mat& regressors, Float64 * /*target_ptr*/, size_t n_rows) override 
    {
        // PODArray<size_t> modified_target;
        // modified_target.resize(n_rows);
        // if (target_ptr == nullptr) 
        //     LOG_FATAL(&Poco::Logger::root(), "\n\n\n\n\n ZHOPA target ptr  {}\n\n\n\n\n", "status");

        
        // for (size_t i = 0; i < n_rows; ++i)
        // {
        //     modified_target[i] = static_cast<size_t>(target_ptr[i]);
        // }
        // size_t * modified_ptr = reinterpret_cast<size_t *>(target_ptr); 
        PODArray<size_t> pipa(n_rows, 1);
        arma::Row<size_t> target = arma::Row<size_t>(pipa.data(), n_rows);
        // arma::Row<size_t> target = arma::Row<size_t>(modified_target.data(), n_rows);
        ens::L_BFGS lbfgsOpt;
        model.Train(regressors, target, 2, lbfgsOpt);
    }

    bool Save(String filepath) override
    {
        return mlpack::data::Save(filepath, "linearsvm", model);
    }

protected:
    mlpack::svm::LinearSVM<> model;    
};

using IModelPtr = std::shared_ptr<IModel>;

}
