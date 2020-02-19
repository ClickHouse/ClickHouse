#include <Common/ThreadProfileEvents.h>

#if defined(__linux__)
#include <unistd.h>
#include <linux/perf_event.h>
#include <syscall.h>
#include <sys/ioctl.h>
#include <cerrno>
#endif

namespace DB {

#if defined(__linux__)

    // todo: think about event counters' overflow
    // todo: ask about the usual error reporting (whether stderr is an accepted way)

    // descriptions' source: http://man7.org/linux/man-pages/man2/perf_event_open.2.html
    const int PerfEventsCounters::perf_event_configs[] = {
            // This reports the CPU clock, a high-resolution per-CPU timer.
//            perf_sw_ids::PERF_COUNT_SW_CPU_CLOCK,
            // This reports a clock count specific to the task that is running.
            perf_sw_ids::PERF_COUNT_SW_TASK_CLOCK,
            // This reports the number of page faults.
            perf_sw_ids::PERF_COUNT_SW_PAGE_FAULTS,
            // This counts context switches.
            // Until Linux 2.6.34, these were all reported as user-space events,
            // after that they are reported as happening in the kernel
            perf_sw_ids::PERF_COUNT_SW_CONTEXT_SWITCHES,
            // This reports the number of times the process has migrated to a new CPU.
            perf_sw_ids::PERF_COUNT_SW_CPU_MIGRATIONS,
            // This counts the number of minor page faults. These did not require disk I/O to handle.
            perf_sw_ids::PERF_COUNT_SW_PAGE_FAULTS_MIN,
            // This counts the number of major page faults. These required disk I/O to handle.
            perf_sw_ids::PERF_COUNT_SW_PAGE_FAULTS_MAJ,
            // This counts the number of alignment faults. These happen when unaligned memory accesses happen;
            // the kernel can handle these but it reduces performance.
            // This happens only on some architectures (never on x86).
            perf_sw_ids::PERF_COUNT_SW_ALIGNMENT_FAULTS,
            // This counts the number of emulation faults. The kernel sometimes traps on unimplemented instructions and
            // emulates them for user space. This can negatively impact performance.
            perf_sw_ids::PERF_COUNT_SW_EMULATION_FAULTS
            // This is a placeholder event that counts nothing. Informational sample record types such as mmap or
            // comm must be associated with an active event. This dummy event allows gathering such records
            // without requiring a counting event.
//            perf_sw_ids::PERF_COUNT_SW_DUMMY
    };

    const ProfileEvents::Event PerfEventsCounters::perf_events[] = {
            // a bit broken according to this: https://stackoverflow.com/a/56967896
//            ProfileEvents::PERF_COUNT_SW_CPU_CLOCK,
            ProfileEvents::PERF_COUNT_SW_TASK_CLOCK,
            ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS,
            ProfileEvents::PERF_COUNT_SW_CONTEXT_SWITCHES,
            ProfileEvents::PERF_COUNT_SW_CPU_MIGRATIONS,
            ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS_MIN,
            ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS_MAJ,
            ProfileEvents::PERF_COUNT_SW_ALIGNMENT_FAULTS,
            ProfileEvents::PERF_COUNT_SW_EMULATION_FAULTS
//            ProfileEvents::PERF_COUNT_SW_DUMMY,
    };

    static_assert(std::size(PerfEventsCounters::perf_event_configs) == PerfEventsCounters::NUMBER_OF_EVENTS);
    static_assert(std::size(PerfEventsCounters::perf_events) == PerfEventsCounters::NUMBER_OF_EVENTS);

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

    static void perfEventStart(int perf_event_paranoid, int perf_event_type, int perf_event_config, int &event_file_descriptor) {
        perf_event_attr pe = perf_event_attr();
        pe.type = perf_event_type;
        pe.size = sizeof(struct perf_event_attr);
        pe.config = perf_event_config;
        // disable by default to add as little extra time as possible
        pe.disabled = 1;
        // can record kernel only when `perf_event_paranoid` <= 1
        pe.exclude_kernel = perf_event_paranoid >= 2;

        event_file_descriptor = openPerfEvent(&pe, /* measure the calling thread */ 0, /* on any cpu */ -1, -1, 0);
    }

    void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters & counters) {
        if (counters.perf_events_recording)
            return;

        int perf_event_paranoid = 0;
        bool is_pref_available = getPerfEventParanoid(perf_event_paranoid);
//        printf("is_perf_available: %s, perf_event_paranoid: %d\n", is_pref_available ? "true" : "false", perf_event_paranoid);

        if (!is_pref_available)
            return;

        for (size_t i = 0; i < NUMBER_OF_EVENTS; ++i)
        {
            perfEventStart(perf_event_paranoid, perf_type_id::PERF_TYPE_SOFTWARE, perf_event_configs[i], counters.events_descriptors[i]);
        }

        for (size_t i = 0; i < NUMBER_OF_EVENTS; ++i)
        {
            int fd = counters.events_descriptors[i];
            if (fd == -1) {
                fprintf(stderr, "Event config %d is unsupported\n", perf_event_configs[i]);
                continue;
            }

            ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
        }

        counters.perf_events_recording = true;
    }

    void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters & counters, ProfileEvents::Counters & profile_events) {
        if (!counters.perf_events_recording)
            return;

        for (size_t i = 0; i < NUMBER_OF_EVENTS; ++i) {
            int & fd = counters.events_descriptors[i];
            if (fd == -1)
                continue;

            long long count;
            read(fd, &count, sizeof(count));

            profile_events.increment(perf_events[i], static_cast<ProfileEvents::Count>(count));
//                printf("%s: %lld\n", perf_event_names[i].c_str(), count);

            if (ioctl(fd, PERF_EVENT_IOC_DISABLE, 0))
                fprintf(stderr, "Can't disable perf event with file descriptor: %d\n", fd);

            if (close(fd))
                fprintf(stderr, "Can't close perf event file descriptor: %d; error: %d - %s\n", fd, errno, strerror(errno));

            fd = -1;
        }

        counters.perf_events_recording = false;
    }

#else

    void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters &) {}
    void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters &, ProfileEvents::Counters &) {}

#endif

}