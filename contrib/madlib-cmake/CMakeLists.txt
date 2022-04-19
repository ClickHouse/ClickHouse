option(ENABLE_MADLIB "Enable madlib" ${ENABLE_LIBRARIES})

if (NOT ENABLE_MADLIB)
    message(STATUS "Not using madlib")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/madlib")

set (SRCS
    "${LIBRARY_DIR}/src/ports/postgres/dbconnector/main.cpp"
    "${LIBRARY_DIR}/src/ports/postgres/dbconnector/NewDelete.cpp"
    "${LIBRARY_DIR}/src/modules/convex/lmf_igd.cpp"
    "${LIBRARY_DIR}/src/modules/convex/mlp_igd.cpp"
    "${LIBRARY_DIR}/src/modules/convex/utils_regularization.cpp"
    "${LIBRARY_DIR}/src/modules/convex/linear_svm_igd.cpp"
    "${LIBRARY_DIR}/src/modules/lda/lda.cpp"
    "${LIBRARY_DIR}/src/modules/glm/multi_response_glm.cpp"
    "${LIBRARY_DIR}/src/modules/glm/glm.cpp"
    "${LIBRARY_DIR}/src/modules/glm/ordinal.cpp"
    "${LIBRARY_DIR}/src/modules/stats/wilcoxon_signed_rank_test.cpp"
    "${LIBRARY_DIR}/src/modules/stats/marginal_cox.cpp"
    "${LIBRARY_DIR}/src/modules/stats/robust_variance_coxph.cpp"
    "${LIBRARY_DIR}/src/modules/stats/one_way_anova.cpp"
    "${LIBRARY_DIR}/src/modules/stats/t_test.cpp"
    "${LIBRARY_DIR}/src/modules/stats/distribution.cpp"
    "${LIBRARY_DIR}/src/modules/stats/coxph_improved.cpp"
    "${LIBRARY_DIR}/src/modules/stats/chi_squared_test.cpp"
    "${LIBRARY_DIR}/src/modules/stats/correlation.cpp"
    "${LIBRARY_DIR}/src/modules/stats/cox_prop_hazards.cpp"
    "${LIBRARY_DIR}/src/modules/stats/mann_whitney_test.cpp"
    "${LIBRARY_DIR}/src/modules/stats/kolmogorov_smirnov_test.cpp"
    "${LIBRARY_DIR}/src/modules/stats/clustered_variance_coxph.cpp"
    "${LIBRARY_DIR}/src/modules/assoc_rules/assoc_rules.cpp"
    "${LIBRARY_DIR}/src/modules/tsa/arima.cpp"
    "${LIBRARY_DIR}/src/modules/prob/student.cpp"
    "${LIBRARY_DIR}/src/modules/prob/boost.cpp"
    "${LIBRARY_DIR}/src/modules/prob/kolmogorov.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/average.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/matrix_ops.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/svd.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/matrix_decomp.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/dim_conversion.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/metric.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/crossprod.cpp"
    "${LIBRARY_DIR}/src/modules/linalg/matrix_agg.cpp"
    "${LIBRARY_DIR}/src/modules/sample/random_process.cpp"
    "${LIBRARY_DIR}/src/modules/sample/weighted_sample.cpp"
    "${LIBRARY_DIR}/src/modules/crf/viterbi.cpp"
    "${LIBRARY_DIR}/src/modules/crf/linear_crf.cpp"
    "${LIBRARY_DIR}/src/modules/linear_systems/dense_linear_systems.cpp"
    "${LIBRARY_DIR}/src/modules/linear_systems/sparse_linear_systems.cpp"
    "${LIBRARY_DIR}/src/modules/elastic_net/elastic_net_gaussian_fista.cpp"
    "${LIBRARY_DIR}/src/modules/elastic_net/elastic_net_binomial_igd.cpp"
    "${LIBRARY_DIR}/src/modules/elastic_net/elastic_net_utils.cpp"
    "${LIBRARY_DIR}/src/modules/elastic_net/elastic_net_binomial_fista.cpp"
    "${LIBRARY_DIR}/src/modules/elastic_net/elastic_net_gaussian_igd.cpp"
    "${LIBRARY_DIR}/src/modules/recursive_partitioning/random_forest.cpp"
    "${LIBRARY_DIR}/src/modules/recursive_partitioning/feature_encoding.cpp"
    "${LIBRARY_DIR}/src/modules/recursive_partitioning/decision_tree.cpp"
    "${LIBRARY_DIR}/src/modules/regress/marginal.cpp"
    "${LIBRARY_DIR}/src/modules/regress/mlogr_margins.cpp"
    "${LIBRARY_DIR}/src/modules/regress/clustered_errors.cpp"
    "${LIBRARY_DIR}/src/modules/regress/linear.cpp"
    "${LIBRARY_DIR}/src/modules/regress/logistic.cpp"
    "${LIBRARY_DIR}/src/modules/regress/multilogistic.cpp"
    "${LIBRARY_DIR}/src/modules/utilities/path.cpp"
    "${LIBRARY_DIR}/src/utils/libstdcxx-compatibility.cpp"
)


add_library(_madlib ${SRCS})

target_include_directories (_madlib SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include" "${LIBRARY_DIR}")

add_library(ch_contrib::madlib ALIAS _madlib)
