#include <Common/ThreadProfileEvents.h>

#if defined(__linux__)
#include <unistd.h>
#include <linux/perf_event.h>
#include <syscall.h>
#include <sys/ioctl.h>
#endif

namespace DB {

#if defined(__linux__)

    static constexpr int perf_event_configs[] = {
            perf_sw_ids::PERF_COUNT_SW_CPU_CLOCK,
            perf_sw_ids::PERF_COUNT_SW_TASK_CLOCK,
            perf_sw_ids::PERF_COUNT_SW_PAGE_FAULTS,
            perf_sw_ids::PERF_COUNT_SW_CONTEXT_SWITCHES,
            perf_sw_ids::PERF_COUNT_SW_CPU_MIGRATIONS,
            perf_sw_ids::PERF_COUNT_SW_PAGE_FAULTS_MIN,
            perf_sw_ids::PERF_COUNT_SW_PAGE_FAULTS_MAJ,
            perf_sw_ids::PERF_COUNT_SW_ALIGNMENT_FAULTS,
            perf_sw_ids::PERF_COUNT_SW_EMULATION_FAULTS,
            perf_sw_ids::PERF_COUNT_SW_DUMMY,
            perf_sw_ids::PERF_COUNT_SW_BPF_OUTPUT
    };

    static const std::string perf_event_names[] = {
            "PERF_COUNT_SW_CPU_CLOCK",
            "PERF_COUNT_SW_TASK_CLOCK",
            "PERF_COUNT_SW_PAGE_FAULTS",
            "PERF_COUNT_SW_CONTEXT_SWITCHES",
            "PERF_COUNT_SW_CPU_MIGRATIONS",
            "PERF_COUNT_SW_PAGE_FAULTS_MIN",
            "PERF_COUNT_SW_PAGE_FAULTS_MAJ",
            "PERF_COUNT_SW_ALIGNMENT_FAULTS",
            "PERF_COUNT_SW_EMULATION_FAULTS",
            "PERF_COUNT_SW_DUMMY",
            "PERF_COUNT_SW_BPF_OUTPUT"
    };

    static const ProfileEvents::Event perf_events[] = {
            ProfileEvents::PERF_COUNT_SW_CPU_CLOCK,
            ProfileEvents::PERF_COUNT_SW_TASK_CLOCK,
            ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS,
            ProfileEvents::PERF_COUNT_SW_CONTEXT_SWITCHES,
            ProfileEvents::PERF_COUNT_SW_CPU_MIGRATIONS,
            ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS_MIN,
            ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS_MAJ,
            ProfileEvents::PERF_COUNT_SW_ALIGNMENT_FAULTS,
            ProfileEvents::PERF_COUNT_SW_EMULATION_FAULTS,
            ProfileEvents::PERF_COUNT_SW_DUMMY,
            ProfileEvents::PERF_COUNT_SW_BPF_OUTPUT
    };

    constexpr size_t NUMBER_OF_EVENTS = std::size(perf_event_configs);

    static_assert(std::size(perf_event_names) == NUMBER_OF_EVENTS);
    static_assert(std::size(perf_events) == NUMBER_OF_EVENTS);

    static int events_descriptors[NUMBER_OF_EVENTS];
    static bool perf_events_opened = false;

    static int openPerfEvent(perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
        return static_cast<int>(syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags));
    }

    static bool getPerfEventParanoid(int &result) {
        // the longest possible variant: "-1\0"
        constexpr int MAX_LENGTH = 3;
        FILE *fp;
        char str[MAX_LENGTH];

        fp = fopen("/proc/sys/kernel/perf_event_paranoid", "r");
        if (fp == nullptr)
            return false;

        char *res = fgets(str, MAX_LENGTH, fp);
        fclose(fp);

        if (res == nullptr)
            return false;

        str[MAX_LENGTH - 1] = '\0';
        // todo: change to `strtol`
        result = atoi(str);
        return true;
    }

    static void perf_event_start(int perf_event_paranoid, int perf_event_type, int perf_event_config, int &event_file_descriptor) {
        perf_event_attr pe = perf_event_attr();
        pe.type = perf_event_type;
        pe.size = sizeof(struct perf_event_attr);
        pe.config = perf_event_config;
        pe.disabled = 1;
        // can record kernel only when `perf_event_paranoid` <= 1
        pe.exclude_kernel = perf_event_paranoid >= 2;
        pe.exclude_hv = 1;

        event_file_descriptor = openPerfEvent(&pe, 0, -1, -1, 0);
    }

//    static void disable_events() {
//        if (!perf_events_opened)
//            return;
//
//        for (int & fd : events_descriptors) {
//            if (fd == -1)
//                continue;
//
//            ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
//            close(fd);
//            fd = -1;
//        }
//
//        perf_events_opened = false;
//    }

    void PerfEventsCounters::updateProfileEvents(ProfileEvents::Counters &profile_events) {
        if (perf_events_opened) {
            for (size_t i = 0; i < NUMBER_OF_EVENTS; ++i) {
                int fd = events_descriptors[i];
                if (fd == -1)
                    continue;

                long long count;
                read(fd, &count, sizeof(count));

                profile_events.increment(perf_events[i], static_cast<ProfileEvents::Count>(count));
//                printf("%s: %lld\n", perf_event_names[i].c_str(), count);

                ioctl(fd, PERF_EVENT_IOC_RESET, 0);
            }

            return;
        }

        int perf_event_paranoid = 0;
        bool is_pref_available = getPerfEventParanoid(perf_event_paranoid);
//        printf("is_perf_available: %s, perf_event_paranoid: %d\n", is_pref_available ? "true" : "false", perf_event_paranoid);

        if (!is_pref_available)
            return;

        for (size_t i = 0; i < NUMBER_OF_EVENTS; ++i)
        {
            int eventConfig = perf_event_configs[i];
            std::string eventName = perf_event_names[i];

            perf_event_start(perf_event_paranoid, perf_type_id::PERF_TYPE_SOFTWARE, eventConfig, events_descriptors[i]);
        }

        for (size_t i = 0; i < NUMBER_OF_EVENTS; ++i)
        {
            int fd = events_descriptors[i];
            if (fd == -1)
                fprintf(stderr, "Event config %d is unsupported\n", perf_event_configs[i]);
            else
                ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
        }

        perf_events_opened = true;
    }

#else

    void PerfEventsCounters::updateProfileEvents(ProfileEvents::Counters &) {}

#endif

}