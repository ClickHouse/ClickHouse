package system

import (
	"strings"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/elastic/gosigar"
	"github.com/jaypipes/ghw"
	"github.com/matishsiao/goInfo"
	"github.com/pkg/errors"
)

// This collector collects the system overview

type SystemCollector struct {
	resourceManager *platform.ResourceManager
}

func NewSystemCollector(m *platform.ResourceManager) *SystemCollector {
	return &SystemCollector{
		resourceManager: m,
	}
}

func (sc *SystemCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	_, err := conf.ValidateConfig(sc.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	frames := make(map[string]data.Frame)
	var frameErrors []error

	frameErrors = addStatsToFrame(frames, frameErrors, "disks", getDisk)
	frameErrors = addStatsToFrame(frames, frameErrors, "disk_usage", getDiskUsage)

	frameErrors = addStatsToFrame(frames, frameErrors, "memory", getMemory)
	frameErrors = addStatsToFrame(frames, frameErrors, "memory_usage", getMemoryUsage)

	frameErrors = addStatsToFrame(frames, frameErrors, "cpu", getCPU)
	//frameErrors = addStatsToFrame(frames, frameErrors, "cpu_usage", getCPUUsage)

	frameErrors = addStatsToFrame(frames, frameErrors, "processes", getProcessList)

	frameErrors = addStatsToFrame(frames, frameErrors, "os", getHostDetails)

	return &data.DiagnosticBundle{
		Frames: frames,
		Errors: data.FrameErrors{
			Errors: frameErrors,
		},
	}, err
}

func addStatsToFrame(frames map[string]data.Frame, errors []error, name string, statFunc func() (data.MemoryFrame, error)) []error {
	frame, err := statFunc()
	if err != nil {
		errors = append(errors, err)
	}
	frames[name] = frame
	return errors
}

func (sc *SystemCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{},
	}
}

func (sc *SystemCollector) IsDefault() bool {
	return true
}

func getDisk() (data.MemoryFrame, error) {
	block, err := ghw.Block()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to list block storage")
	}
	var rows [][]interface{}
	columns := []string{"name", "size", "physicalBlockSize", "driveType", "controller", "vendor", "model", "partitionName", "partitionSize", "mountPoint", "readOnly"}
	for _, disk := range block.Disks {
		for _, part := range disk.Partitions {
			rows = append(rows, []interface{}{disk.Name, disk.SizeBytes, disk.PhysicalBlockSizeBytes, disk.DriveType, disk.StorageController, disk.Vendor, disk.Model, part.Name, part.SizeBytes, part.MountPoint, part.IsReadOnly})
		}
	}
	return data.NewMemoryFrame("disk_usage", columns, rows), nil
}

func getDiskUsage() (data.MemoryFrame, error) {
	fsList := gosigar.FileSystemList{}
	err := fsList.Get()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to list filesystems for usage")
	}
	rows := make([][]interface{}, len(fsList.List))
	columns := []string{"filesystem", "size", "used", "avail", "use%", "mounted on"}
	for i, fs := range fsList.List {
		dirName := fs.DirName
		usage := gosigar.FileSystemUsage{}
		err = usage.Get(dirName)
		if err == nil {
			rows[i] = []interface{}{fs.DevName, usage.Total, usage.Used, usage.Avail, usage.UsePercent(), dirName}
		} else {
			// we try to output something
			rows[i] = []interface{}{fs.DevName, 0, 0, 0, 0, dirName}
		}
	}
	return data.NewMemoryFrame("disk_usage", columns, rows), nil
}

func getMemory() (data.MemoryFrame, error) {
	memory, err := ghw.Memory()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to read memory")
	}
	columns := []string{"totalPhysical", "totalUsable", "supportedPageSizes"}
	rows := make([][]interface{}, 1)
	rows[0] = []interface{}{memory.TotalPhysicalBytes, memory.TotalUsableBytes, memory.SupportedPageSizes}
	return data.NewMemoryFrame("memory", columns, rows), nil
}

func getMemoryUsage() (data.MemoryFrame, error) {
	mem := gosigar.Mem{}
	swap := gosigar.Swap{}

	err := mem.Get()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to read memory usage")
	}

	err = swap.Get()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to read swap")
	}

	columns := []string{"type", "total", "used", "free"}
	rows := make([][]interface{}, 3)

	rows[0] = []interface{}{"mem", mem.Total, mem.Used, mem.Free}
	rows[1] = []interface{}{"buffers/cache", 0, mem.ActualUsed, mem.ActualFree}
	rows[2] = []interface{}{"swap", swap.Total, swap.Used, swap.Free}
	return data.NewMemoryFrame("memory_usage", columns, rows), nil

}

func getCPU() (data.MemoryFrame, error) {
	cpu, err := ghw.CPU()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to list cpus")
	}
	columns := []string{"processor", "vendor", "model", "core", "numThreads", "logical", "capabilities"}
	var rows [][]interface{}
	for _, proc := range cpu.Processors {
		for _, core := range proc.Cores {
			rows = append(rows, []interface{}{proc.ID, proc.Vendor, proc.Model, core.ID, core.NumThreads, core.LogicalProcessors, strings.Join(proc.Capabilities, " ")})
		}
	}
	return data.NewMemoryFrame("cpu", columns, rows), nil
}

// this gets cpu usage vs a listing of arch etc - see getCPU(). This needs successive values as its ticks - not currently used
// see https://github.com/elastic/beats/blob/master/metricbeat/internal/metrics/cpu/metrics.go#L131 for inspiration
//nolint
func getCPUUsage() (data.MemoryFrame, error) {
	cpuList := gosigar.CpuList{}
	err := cpuList.Get()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to list cpus for usage")
	}
	columns := []string{"sys", "nice", "stolen", "irq", "idle", "softIrq", "user", "wait", "total"}
	rows := make([][]interface{}, len(cpuList.List), len(cpuList.List))
	for i, cpu := range cpuList.List {
		rows[i] = []interface{}{cpu.Sys, cpu.Nice, cpu.Stolen, cpu.Irq, cpu.Idle, cpu.SoftIrq, cpu.User, cpu.Wait, cpu.Total()}
	}
	return data.NewMemoryFrame("cpu_usage", columns, rows), nil
}

func getProcessList() (data.MemoryFrame, error) {
	pidList := gosigar.ProcList{}
	err := pidList.Get()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to list processes")
	}
	columns := []string{"pid", "ppid", "stime", "time", "rss", "size", "faults", "minorFaults", "majorFaults", "user", "state", "priority", "nice", "command"}
	rows := make([][]interface{}, len(pidList.List))
	for i, pid := range pidList.List {
		state := gosigar.ProcState{}
		mem := gosigar.ProcMem{}
		time := gosigar.ProcTime{}
		args := gosigar.ProcArgs{}
		if err := state.Get(pid); err != nil {
			continue
		}
		if err := mem.Get(pid); err != nil {
			continue
		}
		if err := time.Get(pid); err != nil {
			continue
		}
		if err := args.Get(pid); err != nil {
			continue
		}
		rows[i] = []interface{}{pid, state.Ppid, time.FormatStartTime(), time.FormatTotal(), mem.Resident, mem.Size,
			mem.PageFaults, mem.MinorFaults, mem.MajorFaults, state.Username, state.State, state.Priority, state.Nice,
			strings.Join(args.List, " ")}
	}
	return data.NewMemoryFrame("process_list", columns, rows), nil
}

func getHostDetails() (data.MemoryFrame, error) {
	gi, err := goInfo.GetInfo()
	if err != nil {
		return data.MemoryFrame{}, errors.Wrapf(err, "unable to get host summary")
	}
	columns := []string{"hostname", "os", "goOs", "cpus", "core", "kernel", "platform"}
	rows := [][]interface{}{
		{gi.Hostname, gi.OS, gi.GoOS, gi.CPUs, gi.Core, gi.Kernel, gi.Platform},
	}
	return data.NewMemoryFrame("os", columns, rows), nil
}

func (sc *SystemCollector) Description() string {
	return "Collects summary OS and hardware statistics for the host"
}

// here we register the collector for use
func init() {
	collectors.Register("system", func() (collectors.Collector, error) {
		return &SystemCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
