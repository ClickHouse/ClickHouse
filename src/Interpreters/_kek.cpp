#include <mlpack/core.hpp>
#include <mlpack/methods/linear_regression/linear_regression.hpp>
#include <Common/logger_useful.h>

void popa() {
    arma::mat regressors({1.0, 2.0, 3.0});
    arma::rowvec responses({1.0, 4.0, 9.0});
    auto lr = mlpack::regression::LinearRegression(regressors, responses);
    arma::mat testX({2.0});
    arma::rowvec testY;
    lr.Predict(testX, testY);
    // std::cout << testY << std::endl;
    
    // bool status = mlpack::data::Save("zhopa.bin", "sraka", lr);

    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", testY);
    // mlpack::regression::LinearRegression lr;
    // bool status = mlpack::data::Load("zhopa.bin", "sraka", lr);
    // lr.Predict(testX, testY);
    // std::cout << status << std::endl;
}
