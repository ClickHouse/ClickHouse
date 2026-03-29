#include <gtest/gtest.h>

#include <Functions/array/arrayRandomProjection.h>

using namespace DB;

// Row-major GEMM: Y(N x D) = X(N x d) * P^T; P is D x d row-major.
template <typename T>
void gemmSimple(const T * X, const T * P, T * Y, size_t N, size_t d, size_t D)
{
    for (size_t i = 0; i < N; ++i)
        for (size_t j = 0; j < D; ++j)
        {
            T sum = T(0);
            for (size_t k = 0; k < d; ++k)
                sum += X[i * d + k] * P[j * d + k];
            Y[i * D + j] = sum;
        }
}

TEST(ArrayRandomProjection, FWHT4)
{
    {
        float data[] = {1.0f, 0.0f, 0.0f, 0.0f};
        fwht(data, 4);
        EXPECT_FLOAT_EQ(data[0], 1.0f);
        EXPECT_FLOAT_EQ(data[1], 1.0f);
        EXPECT_FLOAT_EQ(data[2], 1.0f);
        EXPECT_FLOAT_EQ(data[3], 1.0f);
    }
    {
        float data[] = {1.0f, 1.0f, 1.0f, 1.0f};
        fwht(data, 4);
        EXPECT_FLOAT_EQ(data[0], 4.0f);
        EXPECT_FLOAT_EQ(data[1], 0.0f);
        EXPECT_FLOAT_EQ(data[2], 0.0f);
        EXPECT_FLOAT_EQ(data[3], 0.0f);
    }
    {
        float data[] = {1.0f, 2.0f, 3.0f, 4.0f};
        fwht(data, 4);
        EXPECT_FLOAT_EQ(data[0], 10.0f);
        EXPECT_FLOAT_EQ(data[1], -2.0f);
        EXPECT_FLOAT_EQ(data[2], -4.0f);
        EXPECT_FLOAT_EQ(data[3], 0.0f);
    }
}

TEST(ArrayRandomProjection, FWHT8)
{
    float data[] = {1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
    fwht(data, 8);
    for (float i : data)
        EXPECT_FLOAT_EQ(i, 1.0f);
}

TEST(ArrayRandomProjection, FWHTInverse)
{
    float original[] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
    float data[8];
    std::memcpy(data, original, sizeof(data));

    fwht(data, 8);
    fwht(data, 8);

    for (int i = 0; i < 8; ++i)
        EXPECT_FLOAT_EQ(data[i], 8.0f * original[i]);
}

TEST(ArrayRandomProjection, BoxMullerDistribution)
{
    pcg64 rng(12345);
    const size_t n = 100000;
    double sum = 0;
    double sum_sq = 0;

    for (size_t i = 0; i < n; ++i)
    {
        double val = boxMullerNormal<double>(rng);
        sum += val;
        sum_sq += val * val;
    }

    double mean = sum / n;
    double variance = sum_sq / n - mean * mean;

    EXPECT_NEAR(mean, 0.0, 0.02);
    EXPECT_NEAR(variance, 1.0, 0.05);
}

TEST(ArrayRandomProjection, BoxMullerDeterminism)
{
    const size_t n = 100;

    pcg64 rng1(42);
    pcg64 rng2(42);

    for (size_t i = 0; i < n; ++i)
    {
        float v1 = boxMullerNormal<float>(rng1);
        float v2 = boxMullerNormal<float>(rng2);
        EXPECT_FLOAT_EQ(v1, v2);
    }
}

TEST(ArrayRandomProjection, AchlioptasSparsity)
{
    pcg64 rng(42);
    std::uniform_int_distribution<int> dist6(0, 5);

    const size_t target_dim = 1000;
    const size_t input_dim = 1000;
    size_t total = target_dim * input_dim;
    size_t nonzero = 0;

    for (size_t i = 0; i < total; ++i)
    {
        int val = dist6(rng);
        if (val == 0 || val == 5)
            ++nonzero;
    }

    double sparsity = 1.0 - static_cast<double>(nonzero) / static_cast<double>(total);
    EXPECT_NEAR(sparsity, 2.0 / 3.0, 0.02);
}

// Householder QR: Q^T Q ~ I.
TEST(ArrayRandomProjection, HouseholderQROrthogonalityFloat)
{
    const size_t n = 8;
    pcg64 rng(42);

    std::vector<float> g(n * n);
    for (size_t i = 0; i < n; ++i)
        for (size_t j = 0; j < n; ++j)
            g[i + j * n] = boxMullerNormal<float>(rng);

    std::vector<float> tau(n);
    householderQR<float>(g.data(), tau.data(), n);

    std::vector<float> q(n * n);
    extractQ<float>(g.data(), tau.data(), q.data(), n);

    float max_error = 0;
    for (size_t i = 0; i < n; ++i)
    {
        for (size_t j = 0; j < n; ++j)
        {
            float dot = 0;
            for (size_t k = 0; k < n; ++k)
                dot += q[k + i * n] * q[k + j * n];
            float expected = (i == j) ? 1.0f : 0.0f;
            max_error = std::max(max_error, std::abs(dot - expected));
        }
    }
    EXPECT_LT(max_error, 1e-4f);
}

TEST(ArrayRandomProjection, HouseholderQROrthogonalityDouble)
{
    const size_t n = 32;
    pcg64 rng(99);

    std::vector<double> g(n * n);
    for (size_t i = 0; i < n; ++i)
        for (size_t j = 0; j < n; ++j)
            g[i + j * n] = boxMullerNormal<double>(rng);

    std::vector<double> tau(n);
    householderQR<double>(g.data(), tau.data(), n);

    std::vector<double> q(n * n);
    extractQ<double>(g.data(), tau.data(), q.data(), n);

    double max_error = 0;
    for (size_t i = 0; i < n; ++i)
    {
        for (size_t j = 0; j < n; ++j)
        {
            double dot = 0;
            for (size_t k = 0; k < n; ++k)
                dot += q[k + i * n] * q[k + j * n];
            double expected = (i == j) ? 1.0 : 0.0;
            max_error = std::max(max_error, std::abs(dot - expected));
        }
    }
    EXPECT_LT(max_error, 1e-10);
}

// Reconstruction: A ~ Q * R.
TEST(ArrayRandomProjection, HouseholderQRFactorizationCorrectness)
{
    const size_t n = 8;
    pcg64 rng(77);

    std::vector<float> a_orig(n * n);
    for (size_t i = 0; i < n; ++i)
        for (size_t j = 0; j < n; ++j)
            a_orig[i + j * n] = boxMullerNormal<float>(rng);

    std::vector<float> a(a_orig);
    std::vector<float> tau(n);
    householderQR<float>(a.data(), tau.data(), n);

    std::vector<float> r(n * n, 0.0f);
    for (size_t j = 0; j < n; ++j)
        for (size_t i = 0; i <= j; ++i)
            r[i + j * n] = a[i + j * n];

    std::vector<float> q(n * n);
    extractQ<float>(a.data(), tau.data(), q.data(), n);

    float max_error = 0;
    for (size_t j = 0; j < n; ++j)
    {
        for (size_t i = 0; i < n; ++i)
        {
            float val = 0;
            for (size_t k = 0; k < n; ++k)
                val += q[i + k * n] * r[k + j * n];
            max_error = std::max(max_error, std::abs(val - a_orig[i + j * n]));
        }
    }
    EXPECT_LT(max_error, 1e-4f);
}

// Leading D x d block P: P P^T ~ I.
TEST(ArrayRandomProjection, OrthogonalProjectionProperty)
{
    const size_t d = 16;
    const size_t target_dim = 8;
    const size_t n = std::max(d, target_dim);

    pcg64 rng(42);

    std::vector<float> g(n * n);
    for (size_t i = 0; i < n; ++i)
        for (size_t j = 0; j < n; ++j)
            g[i + j * n] = boxMullerNormal<float>(rng);

    std::vector<float> tau(n);
    householderQR<float>(g.data(), tau.data(), n);

    std::vector<float> q(n * n);
    extractQ<float>(g.data(), tau.data(), q.data(), n);

    std::vector<float> p(target_dim * d);
    for (size_t i = 0; i < target_dim; ++i)
        for (size_t j = 0; j < d; ++j)
            p[i * d + j] = q[i + j * n];

    float max_error = 0;
    for (size_t i = 0; i < target_dim; ++i)
    {
        for (size_t j = 0; j < target_dim; ++j)
        {
            float dot = 0;
            for (size_t k = 0; k < d; ++k)
                dot += p[i * d + k] * p[j * d + k];
            float expected = (i == j) ? 1.0f : 0.0f;
            max_error = std::max(max_error, std::abs(dot - expected));
        }
    }
    EXPECT_LT(max_error, 1e-4f);
}

// OrthogonalProjectionState: P P^T ~ scale^2 * I (sqrt(n/D) scaling in state).
TEST(ArrayRandomProjection, OrthogonalProjectionStatePipeline)
{
    OrthogonalProjectionState<float> state;
    state.generate(64, 16, 42);

    EXPECT_EQ(state.input_dim, 64u);
    EXPECT_EQ(state.target_dim, 16u);
    EXPECT_EQ(state.matrix.size(), 16u * 64u);

    float expected_scale_sq = static_cast<float>(std::max(64u, 16u)) / 16.0f;
    float max_error = 0;
    for (size_t i = 0; i < 16; ++i)
    {
        for (size_t j = 0; j < 16; ++j)
        {
            float dot = 0;
            for (size_t k = 0; k < 64; ++k)
                dot += state.matrix[i * 64 + k] * state.matrix[j * 64 + k];
            float expected = (i == j) ? expected_scale_sq : 0.0f;
            max_error = std::max(max_error, std::abs(dot - expected));
        }
    }
    EXPECT_LT(max_error, 0.1f);
}

// Gaussian random projection: average ||y|| / ||x|| ~ 1 (JL-style).
TEST(ArrayRandomProjection, NormalProjectionNormPreservation)
{
    const size_t d = 64;
    const size_t target_dim = 32;
    const int num_vectors = 100;

    pcg64 rng(42);

    float scale = 1.0f / std::sqrt(static_cast<float>(target_dim));
    std::vector<float> p(target_dim * d);
    for (size_t i = 0; i < target_dim * d; ++i)
        p[i] = boxMullerNormal<float>(rng) * scale;

    pcg64 vec_rng(123);
    double total_ratio = 0;
    for (int v = 0; v < num_vectors; ++v)
    {
        std::vector<float> x(d);
        for (size_t i = 0; i < d; ++i)
            x[i] = boxMullerNormal<float>(vec_rng);

        std::vector<float> y(target_dim);
        gemmSimple<float>(x.data(), p.data(), y.data(), 1, d, target_dim);

        float orig_norm = std::sqrt(dotProductScalar<float>(x.data(), x.data(), d));
        float proj_norm = std::sqrt(dotProductScalar<float>(y.data(), y.data(), target_dim));

        if (orig_norm > 1e-6f)
            total_ratio += proj_norm / orig_norm;
    }

    double avg_ratio = total_ratio / num_vectors;
    EXPECT_NEAR(avg_ratio, 1.0, 0.15);
}

// GEMM: tiled scalar and SIMD dispatch match reference; odd d exercises tails.
TEST(ArrayRandomProjection, GEMMCorrectness)
{
    const size_t n = 5;
    const size_t d = 17;
    const size_t dd = 7;

    pcg64 rng(42);

    std::vector<float> x(n * d);
    std::vector<float> p(dd * d);
    for (auto & v : x)
        v = boxMullerNormal<float>(rng);
    for (auto & v : p)
        v = boxMullerNormal<float>(rng);

    std::vector<float> y_ref(n * dd);
    gemmSimple<float>(x.data(), p.data(), y_ref.data(), n, d, dd);

    float sum = 0;
    for (auto v : y_ref)
        sum += std::abs(v);
    EXPECT_GT(sum, 0.0f);

    std::vector<float> y_scalar(n * dd);
    gemmBatchScalar<float>(x.data(), p.data(), y_scalar.data(), n, d, dd);

    for (size_t i = 0; i < n * dd; ++i)
        EXPECT_NEAR(y_scalar[i], y_ref[i], 1e-4f) << "scalar mismatch at index " << i;

    std::vector<float> y_dispatch(n * dd);
    applyDenseProjectionBatch<float>(x.data(), y_dispatch.data(), n, d, dd, p.data());

    for (size_t i = 0; i < n * dd; ++i)
        EXPECT_NEAR(y_dispatch[i], y_ref[i], 1e-4f) << "dispatch mismatch at index " << i;
}

// Large GEMM: SIMD main loop vs reference.
TEST(ArrayRandomProjection, GEMMCorrectnessLarge)
{
    const size_t n = 10;
    const size_t d = 128;
    const size_t dd = 64;

    pcg64 rng(123);

    std::vector<float> x(n * d);
    std::vector<float> p(dd * d);
    for (auto & v : x)
        v = boxMullerNormal<float>(rng);
    for (auto & v : p)
        v = boxMullerNormal<float>(rng);

    std::vector<float> y_ref(n * dd);
    gemmSimple<float>(x.data(), p.data(), y_ref.data(), n, d, dd);

    std::vector<float> y_dispatch(n * dd);
    applyDenseProjectionBatch<float>(x.data(), y_dispatch.data(), n, d, dd, p.data());

    for (size_t i = 0; i < n * dd; ++i)
        EXPECT_NEAR(y_dispatch[i], y_ref[i], 1e-2f) << "large GEMM mismatch at index " << i;
}

TEST(ArrayRandomProjection, NormalProjectionStateDeterminism)
{
    NormalProjectionState<float> s1;
    NormalProjectionState<float> s2;
    s1.generate(128, 32, 42);
    s2.generate(128, 32, 42);

    ASSERT_EQ(s1.matrix.size(), s2.matrix.size());
    for (size_t i = 0; i < s1.matrix.size(); ++i)
        EXPECT_FLOAT_EQ(s1.matrix[i], s2.matrix[i]);
}

TEST(ArrayRandomProjection, SparseProjectionDeterminism)
{
    SparseProjectionState<float> s1;
    SparseProjectionState<float> s2;
    s1.generate(64, 16, 42);
    s2.generate(64, 16, 42);

    std::vector<float> input(64);
    pcg64 rng(99);
    for (auto & v : input)
        v = boxMullerNormal<float>(rng);

    std::vector<float> out1(16);
    std::vector<float> out2(16);
    applySparseProjection<float>(input.data(), out1.data(), s1);
    applySparseProjection<float>(input.data(), out2.data(), s2);

    for (size_t i = 0; i < 16; ++i)
        EXPECT_FLOAT_EQ(out1[i], out2[i]);
}

TEST(ArrayRandomProjection, HadamardProjectionDeterminism)
{
    HadamardProjectionState<float> s1;
    HadamardProjectionState<float> s2;
    s1.generate(64, 16, 42);
    s2.generate(64, 16, 42);

    std::vector<float> input(64);
    pcg64 rng(99);
    for (auto & v : input)
        v = boxMullerNormal<float>(rng);

    std::vector<float> out1(16);
    std::vector<float> out2(16);
    applyHadamardProjection<float>(input.data(), out1.data(), s1);
    applyHadamardProjection<float>(input.data(), out2.data(), s2);

    for (size_t i = 0; i < 16; ++i)
        EXPECT_FLOAT_EQ(out1[i], out2[i]);
}

// dotProduct: scalar vs dispatch for various sizes including SIMD boundaries.
TEST(ArrayRandomProjection, DotProductScalarVsDispatch)
{
    pcg64 rng(42);
    for (size_t n : {0, 1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 128, 255, 256, 257})
    {
        std::vector<float> a(n);
        std::vector<float> b(n);
        for (auto & v : a)
            v = boxMullerNormal<float>(rng);
        for (auto & v : b)
            v = boxMullerNormal<float>(rng);

        float ref = dotProductScalar<float>(a.data(), b.data(), n);
        float got = dotProductDispatch<float>(a.data(), b.data(), n);
        EXPECT_NEAR(got, ref, std::abs(ref) * 1e-5f + 1e-7f) << "n=" << n;
    }

    for (size_t n : {0, 1, 3, 4, 7, 8, 9, 15, 16, 64, 128})
    {
        std::vector<double> a(n);
        std::vector<double> b(n);
        for (auto & v : a)
            v = boxMullerNormal<double>(rng);
        for (auto & v : b)
            v = boxMullerNormal<double>(rng);

        double ref = dotProductScalar<double>(a.data(), b.data(), n);
        double got = dotProductDispatch<double>(a.data(), b.data(), n);
        EXPECT_NEAR(got, ref, std::abs(ref) * 1e-12 + 1e-15) << "n=" << n;
    }
}

// axpy: scalar vs dispatch for various sizes.
TEST(ArrayRandomProjection, AxpyScalarVsDispatch)
{
    pcg64 rng(42);
    for (size_t n : {0, 1, 7, 8, 9, 15, 16, 17, 32, 64, 128, 255})
    {
        std::vector<float> x(n);
        for (auto & v : x)
            v = boxMullerNormal<float>(rng);
        float alpha = boxMullerNormal<float>(rng);

        std::vector<float> y_ref(n, 1.0f);
        std::vector<float> y_got(n, 1.0f);

        axpyScalar<float>(y_ref.data(), x.data(), alpha, n);
        axpyDispatch<float>(y_got.data(), x.data(), alpha, n);

        for (size_t i = 0; i < n; ++i)
            EXPECT_NEAR(y_got[i], y_ref[i], std::abs(y_ref[i]) * 1e-5f + 1e-7f) << "n=" << n << " i=" << i;
    }
}

// nextPowerOfTwo: edge cases.
TEST(ArrayRandomProjection, NextPowerOfTwo)
{
    EXPECT_EQ(nextPowerOfTwo(0), 1u);
    EXPECT_EQ(nextPowerOfTwo(1), 1u);
    EXPECT_EQ(nextPowerOfTwo(2), 2u);
    EXPECT_EQ(nextPowerOfTwo(3), 4u);
    EXPECT_EQ(nextPowerOfTwo(4), 4u);
    EXPECT_EQ(nextPowerOfTwo(5), 8u);
    EXPECT_EQ(nextPowerOfTwo(7), 8u);
    EXPECT_EQ(nextPowerOfTwo(8), 8u);
    EXPECT_EQ(nextPowerOfTwo(9), 16u);
    EXPECT_EQ(nextPowerOfTwo(1023), 1024u);
    EXPECT_EQ(nextPowerOfTwo(1024), 1024u);
    EXPECT_EQ(nextPowerOfTwo(1025), 2048u);
}

// GEMM corner cases: N=1, d=1, D=1.
TEST(ArrayRandomProjection, GEMMCornerCases)
{
    pcg64 rng(42);

    // Single element: 1x1 * 1x1
    {
        float x[] = {3.0f};
        float p[] = {2.0f};
        float y_ref[1];
        float y_got[1];
        gemmSimple<float>(x, p, y_ref, 1, 1, 1);
        applyDenseProjectionBatch<float>(x, y_got, 1, 1, 1, p);
        EXPECT_NEAR(y_got[0], y_ref[0], 1e-6f);
        EXPECT_NEAR(y_got[0], 6.0f, 1e-6f);
    }

    // Single row, multiple columns
    {
        const size_t d = 33;
        const size_t dd = 17;
        std::vector<float> x(d);
        std::vector<float> p(dd * d);
        for (auto & v : x)
            v = boxMullerNormal<float>(rng);
        for (auto & v : p)
            v = boxMullerNormal<float>(rng);

        std::vector<float> y_ref(dd);
        std::vector<float> y_got(dd);
        gemmSimple<float>(x.data(), p.data(), y_ref.data(), 1, d, dd);
        applyDenseProjectionBatch<float>(x.data(), y_got.data(), 1, d, dd, p.data());

        for (size_t i = 0; i < dd; ++i)
            EXPECT_NEAR(y_got[i], y_ref[i], 1e-4f);
    }

    // D=1: single output per row
    {
        const size_t n = 10;
        const size_t d = 64;
        std::vector<float> x(n * d);
        std::vector<float> p(d);
        for (auto & v : x)
            v = boxMullerNormal<float>(rng);
        for (auto & v : p)
            v = boxMullerNormal<float>(rng);

        std::vector<float> y_ref(n);
        std::vector<float> y_got(n);
        gemmSimple<float>(x.data(), p.data(), y_ref.data(), n, d, 1);
        applyDenseProjectionBatch<float>(x.data(), y_got.data(), n, d, 1, p.data());

        for (size_t i = 0; i < n; ++i)
            EXPECT_NEAR(y_got[i], y_ref[i], 1e-4f);
    }
}

// GEMM Float64: dispatch vs reference.
TEST(ArrayRandomProjection, GEMMCorrectnessFloat64)
{
    const size_t n = 5;
    const size_t d = 33;
    const size_t dd = 17;

    pcg64 rng(42);

    std::vector<double> x(n * d);
    std::vector<double> p(dd * d);
    for (auto & v : x)
        v = boxMullerNormal<double>(rng);
    for (auto & v : p)
        v = boxMullerNormal<double>(rng);

    std::vector<double> y_ref(n * dd);
    gemmSimple<double>(x.data(), p.data(), y_ref.data(), n, d, dd);

    std::vector<double> y_got(n * dd);
    applyDenseProjectionBatch<double>(x.data(), y_got.data(), n, d, dd, p.data());

    for (size_t i = 0; i < n * dd; ++i)
        EXPECT_NEAR(y_got[i], y_ref[i], std::abs(y_ref[i]) * 1e-10 + 1e-12) << "i=" << i;
}

// Sparse projection: output is non-zero and has correct dimension.
TEST(ArrayRandomProjection, SparseProjectionCorrectness)
{
    SparseProjectionState<float> state;
    state.generate(64, 16, 42);

    std::vector<float> input(64, 1.0f);
    std::vector<float> output(16, 0.0f);
    applySparseProjection<float>(input.data(), output.data(), state);

    float sum = 0;
    for (auto v : output)
        sum += std::abs(v);
    EXPECT_GT(sum, 0.0f);

    // Zero input -> zero output
    std::vector<float> zero_input(64, 0.0f);
    std::vector<float> zero_output(16, 999.0f);
    applySparseProjection<float>(zero_input.data(), zero_output.data(), state);

    for (size_t i = 0; i < 16; ++i)
        EXPECT_FLOAT_EQ(zero_output[i], 0.0f);
}

// Hadamard projection: output is non-zero and has correct dimension.
TEST(ArrayRandomProjection, HadamardProjectionCorrectness)
{
    HadamardProjectionState<float> state;
    state.generate(64, 16, 42);

    EXPECT_EQ(state.padded_dim, 64u);

    std::vector<float> input(64, 1.0f);
    std::vector<float> output(16, 0.0f);
    applyHadamardProjection<float>(input.data(), output.data(), state);

    float sum = 0;
    for (auto v : output)
        sum += std::abs(v);
    EXPECT_GT(sum, 0.0f);

    // Non-power-of-2 input
    HadamardProjectionState<float> state2;
    state2.generate(50, 10, 42);
    EXPECT_EQ(state2.padded_dim, 64u);

    std::vector<float> input2(50);
    pcg64 rng(99);
    for (auto & v : input2)
        v = boxMullerNormal<float>(rng);

    std::vector<float> output2(10, 0.0f);
    applyHadamardProjection<float>(input2.data(), output2.data(), state2);

    float sum2 = 0;
    for (auto v : output2)
        sum2 += std::abs(v);
    EXPECT_GT(sum2, 0.0f);
}
