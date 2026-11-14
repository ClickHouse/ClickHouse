#include <GPU/GPUActivity.h>

#if USE_GPU

#include <GPU/GPUMemoryTracking.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <cupti_pmsampling.h>
#include <cupti_profiler_host.h>
#include <cupti_profiler_target.h>
#include <cupti_target.h>

#include <dlfcn.h>

#include <array>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

namespace ProfileEvents
{
    extern const Event GPUSMActiveMicroseconds;
    extern const Event GPUTensorActiveMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

namespace
{

/// The functions of CUPTI the sampler calls, loaded with the library.
#define GPU_CUPTI_FUNCTIONS(M) \
    M(cuptiGetResultString) \
    M(cuptiProfilerInitialize) \
    M(cuptiProfilerDeInitialize) \
    M(cuptiDeviceGetChipName) \
    M(cuptiPmSamplingGetCounterAvailability) \
    M(cuptiProfilerHostInitialize) \
    M(cuptiProfilerHostDeinitialize) \
    M(cuptiProfilerHostConfigAddMetrics) \
    M(cuptiProfilerHostGetConfigImageSize) \
    M(cuptiProfilerHostGetConfigImage) \
    M(cuptiProfilerHostEvaluateToGpuValues) \
    M(cuptiPmSamplingEnable) \
    M(cuptiPmSamplingDisable) \
    M(cuptiPmSamplingSetConfig) \
    M(cuptiPmSamplingGetCounterDataSize) \
    M(cuptiPmSamplingCounterDataImageInitialize) \
    M(cuptiPmSamplingStart) \
    M(cuptiPmSamplingStop) \
    M(cuptiPmSamplingDecodeData) \
    M(cuptiPmSamplingGetCounterDataInfo) \
    M(cuptiPmSamplingCounterDataGetSampleInfo)

struct Cupti
{
    void * library = nullptr;
#define GPU_CUPTI_DECLARE(name) decltype(&::name) name = nullptr;
    GPU_CUPTI_FUNCTIONS(GPU_CUPTI_DECLARE)
#undef GPU_CUPTI_DECLARE

    /// Null with the reason in `error` if the library or one of its functions is missing.
    static std::unique_ptr<Cupti> load(std::string & error)
    {
        auto cupti = std::make_unique<Cupti>();
        for (const char * name : {"libcupti.so.13", "libcupti.so"})
            if ((cupti->library = dlopen(name, RTLD_NOW | RTLD_LOCAL)))
                break;
        if (!cupti->library)
        {
            const char * reason = dlerror();
            error = std::string("cannot load libcupti.so of the CUDA toolkit: ") + (reason ? reason : "unknown error");
            return nullptr;
        }
#define GPU_CUPTI_LOAD(name) \
        if (!(cupti->name = reinterpret_cast<decltype(cupti->name)>(dlsym(cupti->library, #name)))) \
        { \
            error = "libcupti.so has no " #name; \
            return nullptr; \
        }
        GPU_CUPTI_FUNCTIONS(GPU_CUPTI_LOAD)
#undef GPU_CUPTI_LOAD
        return cupti;
    }

    ~Cupti()
    {
        if (library)
            dlclose(library);
    }

    std::string describe(CUptiResult result) const
    {
        const char * text = nullptr;
        if (cuptiGetResultString && cuptiGetResultString(result, &text) == CUPTI_SUCCESS && text)
            return text;
        return "CUPTI error " + std::to_string(result);
    }
};

#undef GPU_CUPTI_FUNCTIONS

/// The parameters of a call of CUPTI: zeroed, but for their size, which CUPTI checks.
template <typename Params>
Params cuptiParams(size_t size)
{
    Params params{};
    params.structSize = size;
    return params;
}

/// The share of the active SMs and of the active cycles of the tensor pipes, both percents of the whole device.
constexpr std::array<const char *, 2> metric_names = {
    "sm__cycles_active.avg.pct_of_peak_sustained_elapsed",
    "sm__pipe_tensor_cycles_active.avg.pct_of_peak_sustained_elapsed",
};

/// Samples the counter data image holds between two decodings, which come every `decode_interval`.
constexpr std::chrono::milliseconds decode_interval{100};
constexpr size_t hardware_buffer_bytes = 64 << 20;

}

struct GPUActivitySampler::State
{
    std::unique_ptr<Cupti> cupti;
    CUpti_Profiler_Host_Object * host = nullptr;
    CUpti_PmSampling_Object * sampler = nullptr;
    bool profiler_initialized = false;
    bool started = false;
    uint32_t max_samples = 0;
    /// CUPTI keeps pointers to these rather than copies, so they live as long as the sampling.
    std::vector<const char *> metrics{metric_names.begin(), metric_names.end()};
    std::vector<uint8_t> availability_image;
    std::vector<uint8_t> config;
    std::vector<uint8_t> counter_data;

    std::mutex mutex;
    std::condition_variable wake;
    bool stop = false;
    std::thread thread;

    LoggerPtr log = getLogger("GPUActivitySampler");

    void check(CUptiResult result, const char * what) const
    {
        if (result != CUPTI_SUCCESS)
            throw Exception(ErrorCodes::GPU_ERROR, "{}: {}", what, cupti->describe(result));
    }

    void start(uint64_t interval_ms)
    {
        auto initialize = cuptiParams<CUpti_Profiler_Initialize_Params>(CUpti_Profiler_Initialize_Params_STRUCT_SIZE);
        check(cupti->cuptiProfilerInitialize(&initialize), "Cannot initialize the profiler of CUPTI");
        profiler_initialized = true;

        auto chip = cuptiParams<CUpti_Device_GetChipName_Params>(CUpti_Device_GetChipName_Params_STRUCT_SIZE);
        chip.deviceIndex = 0;
        check(cupti->cuptiDeviceGetChipName(&chip), "Cannot tell the chip of the device");

        auto availability = cuptiParams<CUpti_PmSampling_GetCounterAvailability_Params>(CUpti_PmSampling_GetCounterAvailability_Params_STRUCT_SIZE);
        availability.deviceIndex = 0;
        check(cupti->cuptiPmSamplingGetCounterAvailability(&availability), "Cannot read the performance counters of the device");
        availability_image.resize(availability.counterAvailabilityImageSize);
        availability.pCounterAvailabilityImage = availability_image.data();
        check(cupti->cuptiPmSamplingGetCounterAvailability(&availability), "Cannot read the performance counters of the device");

        auto host_params = cuptiParams<CUpti_Profiler_Host_Initialize_Params>(CUpti_Profiler_Host_Initialize_Params_STRUCT_SIZE);
        host_params.profilerType = CUPTI_PROFILER_TYPE_PM_SAMPLING;
        host_params.pChipName = chip.pChipName;
        host_params.pCounterAvailabilityImage = availability_image.data();
        check(cupti->cuptiProfilerHostInitialize(&host_params), "Cannot initialize the host profiler of CUPTI");
        host = host_params.pHostObject;

        auto add = cuptiParams<CUpti_Profiler_Host_ConfigAddMetrics_Params>(CUpti_Profiler_Host_ConfigAddMetrics_Params_STRUCT_SIZE);
        add.pHostObject = host;
        add.ppMetricNames = metrics.data();
        add.numMetrics = metrics.size();
        check(cupti->cuptiProfilerHostConfigAddMetrics(&add), "Cannot configure the metrics of the device");
        auto config_size = cuptiParams<CUpti_Profiler_Host_GetConfigImageSize_Params>(CUpti_Profiler_Host_GetConfigImageSize_Params_STRUCT_SIZE);
        config_size.pHostObject = host;
        check(cupti->cuptiProfilerHostGetConfigImageSize(&config_size), "Cannot configure the metrics of the device");
        config.resize(config_size.configImageSize);
        auto config_image = cuptiParams<CUpti_Profiler_Host_GetConfigImage_Params>(CUpti_Profiler_Host_GetConfigImage_Params_STRUCT_SIZE);
        config_image.pHostObject = host;
        config_image.configImageSize = config.size();
        config_image.pConfigImage = config.data();
        check(cupti->cuptiProfilerHostGetConfigImage(&config_image), "Cannot configure the metrics of the device");

        auto enable = cuptiParams<CUpti_PmSampling_Enable_Params>(CUpti_PmSampling_Enable_Params_STRUCT_SIZE);
        enable.deviceIndex = 0;
        check(cupti->cuptiPmSamplingEnable(&enable), "Cannot enable the PM sampling of the device");
        sampler = enable.pPmSamplingObject;

        auto set_config = cuptiParams<CUpti_PmSampling_SetConfig_Params>(CUpti_PmSampling_SetConfig_Params_STRUCT_SIZE);
        set_config.pPmSamplingObject = sampler;
        set_config.configSize = config.size();
        set_config.pConfig = config.data();
        set_config.hardwareBufferSize = hardware_buffer_bytes;
        set_config.samplingInterval = interval_ms * 1000000;
        set_config.triggerMode = CUPTI_PM_SAMPLING_TRIGGER_MODE_GPU_TIME_INTERVAL;
        set_config.hwBufferAppendMode = CUPTI_PM_SAMPLING_HARDWARE_BUFFER_APPEND_MODE_KEEP_LATEST;
        check(cupti->cuptiPmSamplingSetConfig(&set_config), "Cannot configure the PM sampling of the device");

        /// Twice the samples of a decoding interval; more are decoded in several rounds.
        max_samples = static_cast<uint32_t>(std::max<uint64_t>(2 * decode_interval.count() / interval_ms, 16));
        auto data_size = cuptiParams<CUpti_PmSampling_GetCounterDataSize_Params>(CUpti_PmSampling_GetCounterDataSize_Params_STRUCT_SIZE);
        data_size.pPmSamplingObject = sampler;
        data_size.pMetricNames = metrics.data();
        data_size.numMetrics = metrics.size();
        data_size.maxSamples = max_samples;
        check(cupti->cuptiPmSamplingGetCounterDataSize(&data_size), "Cannot size the counter data of the PM sampling");
        counter_data.resize(data_size.counterDataSize);
        resetCounterData();

        auto start_params = cuptiParams<CUpti_PmSampling_Start_Params>(CUpti_PmSampling_Start_Params_STRUCT_SIZE);
        start_params.pPmSamplingObject = sampler;
        check(cupti->cuptiPmSamplingStart(&start_params), "Cannot start the PM sampling of the device");
        started = true;
    }

    void resetCounterData()
    {
        auto initialize = cuptiParams<CUpti_PmSampling_CounterDataImage_Initialize_Params>(CUpti_PmSampling_CounterDataImage_Initialize_Params_STRUCT_SIZE);
        initialize.pPmSamplingObject = sampler;
        initialize.counterDataSize = counter_data.size();
        initialize.pCounterData = counter_data.data();
        check(cupti->cuptiPmSamplingCounterDataImageInitialize(&initialize), "Cannot initialize the counter data of the PM sampling");
    }

    /// Decodes the samples taken since the last call, in rounds of at most `max_samples`, and accounts their active time.
    void decode()
    {
        double sm_active_us = 0;
        double tensor_active_us = 0;
        std::array<double, metric_names.size()> values{};

        for (bool more = true; more;)
        {
            auto decode_params = cuptiParams<CUpti_PmSampling_DecodeData_Params>(CUpti_PmSampling_DecodeData_Params_STRUCT_SIZE);
            decode_params.pPmSamplingObject = sampler;
            decode_params.pCounterDataImage = counter_data.data();
            decode_params.counterDataImageSize = counter_data.size();
            check(cupti->cuptiPmSamplingDecodeData(&decode_params), "Cannot decode the PM samples of the device");
            more = decode_params.decodeStopReason == CUPTI_PM_SAMPLING_DECODE_STOP_REASON_COUNTER_DATA_FULL;

            auto info = cuptiParams<CUpti_PmSampling_GetCounterDataInfo_Params>(CUpti_PmSampling_GetCounterDataInfo_Params_STRUCT_SIZE);
            info.pCounterDataImage = counter_data.data();
            info.counterDataImageSize = counter_data.size();
            check(cupti->cuptiPmSamplingGetCounterDataInfo(&info), "Cannot read the PM samples of the device");

            for (size_t sample = 0; sample < info.numCompletedSamples; ++sample)
            {
                auto sample_info = cuptiParams<CUpti_PmSampling_CounterData_GetSampleInfo_Params>(CUpti_PmSampling_CounterData_GetSampleInfo_Params_STRUCT_SIZE);
                sample_info.pPmSamplingObject = sampler;
                sample_info.pCounterDataImage = counter_data.data();
                sample_info.counterDataImageSize = counter_data.size();
                sample_info.sampleIndex = sample;
                check(cupti->cuptiPmSamplingCounterDataGetSampleInfo(&sample_info), "Cannot read a PM sample of the device");

                auto evaluate = cuptiParams<CUpti_Profiler_Host_EvaluateToGpuValues_Params>(CUpti_Profiler_Host_EvaluateToGpuValues_Params_STRUCT_SIZE);
                evaluate.pHostObject = host;
                evaluate.pCounterDataImage = counter_data.data();
                evaluate.counterDataImageSize = counter_data.size();
                evaluate.rangeIndex = sample;
                evaluate.ppMetricNames = metrics.data();
                evaluate.numMetrics = metrics.size();
                evaluate.pMetricValues = values.data();
                check(cupti->cuptiProfilerHostEvaluateToGpuValues(&evaluate), "Cannot evaluate a PM sample of the device");

                if (sample_info.endTimestamp <= sample_info.startTimestamp)
                    continue;
                const double duration_us = static_cast<double>(sample_info.endTimestamp - sample_info.startTimestamp) / 1000;
                sm_active_us += duration_us * std::clamp(values[0], 0.0, 100.0) / 100;
                tensor_active_us += duration_us * std::clamp(values[1], 0.0, 100.0) / 100;
            }
            resetCounterData();
        }

        account(sm_active_us, tensor_active_us);
    }

    /// To the queries holding device memory now, in equal shares.
    static void account(double sm_active_us, double tensor_active_us)
    {
        if (sm_active_us <= 0 && tensor_active_us <= 0)
            return;
        const auto groups = groupsHoldingDeviceMemory();
        if (groups.empty())
            return;
        const auto share = static_cast<double>(groups.size());
        for (const auto & group : groups)
        {
            group->performance_counters.increment(ProfileEvents::GPUSMActiveMicroseconds,
                static_cast<ProfileEvents::Count>(sm_active_us / share));
            group->performance_counters.increment(ProfileEvents::GPUTensorActiveMicroseconds,
                static_cast<ProfileEvents::Count>(tensor_active_us / share));
        }
    }

    void run()
    {
        DB::setThreadName(ThreadName::GPU_ACTIVITY);
        std::unique_lock lock(mutex);
        while (!wake.wait_for(lock, decode_interval, [this] { return stop; }))
        {
            lock.unlock();
            try
            {
                decode();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Cannot sample the activity of the GPU");
            }
            lock.lock();
        }
    }

    ~State()
    {
        if (thread.joinable())
        {
            {
                std::lock_guard lock(mutex);
                stop = true;
            }
            wake.notify_all();
            thread.join();
        }
        if (started)
        {
            auto stop_params = cuptiParams<CUpti_PmSampling_Stop_Params>(CUpti_PmSampling_Stop_Params_STRUCT_SIZE);
            stop_params.pPmSamplingObject = sampler;
            cupti->cuptiPmSamplingStop(&stop_params);
        }
        if (sampler)
        {
            auto disable = cuptiParams<CUpti_PmSampling_Disable_Params>(CUpti_PmSampling_Disable_Params_STRUCT_SIZE);
            disable.pPmSamplingObject = sampler;
            cupti->cuptiPmSamplingDisable(&disable);
        }
        if (host)
        {
            auto deinitialize = cuptiParams<CUpti_Profiler_Host_Deinitialize_Params>(CUpti_Profiler_Host_Deinitialize_Params_STRUCT_SIZE);
            deinitialize.pHostObject = host;
            cupti->cuptiProfilerHostDeinitialize(&deinitialize);
        }
        if (profiler_initialized)
        {
            auto deinitialize = cuptiParams<CUpti_Profiler_DeInitialize_Params>(CUpti_Profiler_DeInitialize_Params_STRUCT_SIZE);
            cupti->cuptiProfilerDeInitialize(&deinitialize);
        }
    }
};

GPUActivitySampler::GPUActivitySampler(std::unique_ptr<State> state_)
    : state(std::move(state_))
{
}

GPUActivitySampler::~GPUActivitySampler() = default;

std::unique_ptr<GPUActivitySampler> GPUActivitySampler::tryStart(uint64_t interval_ms)
{
    auto log = getLogger("GPUActivitySampler");
    if (interval_ms == 0)
        return nullptr;

    auto state = std::make_unique<State>();
    std::string error;
    state->cupti = Cupti::load(error);
    if (!state->cupti)
    {
        LOG_INFO(log, "The activity of the GPU is not sampled: {}", error);
        return nullptr;
    }

    try
    {
        state->start(interval_ms);
    }
    catch (...)
    {
        LOG_INFO(log, "The activity of the GPU is not sampled: {}. Reading the performance counters of the GPU needs "
            "CAP_SYS_ADMIN (`setcap cap_sys_admin+ep` on the binary) or the NVIDIA driver loaded with "
            "`NVreg_RestrictProfilingToAdminUsers=0`", getCurrentExceptionMessage(false));
        return nullptr;
    }

    state->thread = std::thread([raw = state.get()] { raw->run(); });
    LOG_INFO(log, "Sampling the activity of the GPU every {} ms", interval_ms);
    return std::unique_ptr<GPUActivitySampler>(new GPUActivitySampler(std::move(state)));
}

}

#endif
