package file

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/outputs"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/mholt/archiver/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const OutputName = "simple"

type SubFolderGenerator func() string

type SimpleOutput struct {
	// mainly used for testing to make sub folder deterministic - which it won't be by default as it uses a timestamp
	FolderGenerator SubFolderGenerator
}

func (o SimpleOutput) Write(id string, bundles map[string]*data.DiagnosticBundle, conf config.Configuration) (data.FrameErrors, error) {
	conf, err := conf.ValidateConfig(o.Configuration())
	if err != nil {
		return data.FrameErrors{}, err
	}
	directory, err := config.ReadStringValue(conf, "directory")
	if err != nil {
		return data.FrameErrors{}, err
	}
	directory, err = getWorkingDirectory(directory)
	if err != nil {
		return data.FrameErrors{}, err
	}
	subFolder := strconv.FormatInt(utils.MakeTimestamp(), 10)
	if o.FolderGenerator != nil {
		subFolder = o.FolderGenerator()
	}
	skipArchive, err := config.ReadBoolValue(conf, "skip_archive")
	if err != nil {
		return data.FrameErrors{}, err
	}

	outputDir := filepath.Join(directory, id, subFolder)
	log.Info().Msgf("creating bundle in %s", outputDir)
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return data.FrameErrors{}, err
	}
	frameErrors := data.FrameErrors{}
	var filePaths []string
	for name := range bundles {
		bundlePaths, frameError := writeDiagnosticBundle(name, bundles[name], outputDir)
		filePaths = append(filePaths, bundlePaths...)
		frameErrors.Errors = append(frameErrors.Errors, frameError.Errors...)
	}
	log.Info().Msg("bundle created")
	if !skipArchive {
		archiveFilename := filepath.Join(directory, id, fmt.Sprintf("%s.tar.gz", subFolder))
		log.Info().Msgf("compressing bundle to %s", archiveFilename)
		// produce a map containing the input paths to the archive paths - we preserve the output directory and hierarchy
		archiveMap := createArchiveMap(filePaths, directory)
		if err := createArchive(archiveFilename, archiveMap); err != nil {
			return frameErrors, err
		}
		// we delete the original directory leaving just the archive behind
		if err := os.RemoveAll(outputDir); err != nil {
			return frameErrors, err
		}
		log.Info().Msgf("archive ready at: %s ", archiveFilename)
	}
	return frameErrors, nil
}

func writeDiagnosticBundle(name string, diag *data.DiagnosticBundle, baseDir string) ([]string, data.FrameErrors) {
	diagDir := filepath.Join(baseDir, name)
	if err := os.MkdirAll(diagDir, os.ModePerm); err != nil {
		return nil, data.FrameErrors{Errors: []error{
			errors.Wrapf(err, "unable to create directory for %s", name),
		}}
	}
	frameErrors := data.FrameErrors{}
	var filePaths []string
	for frameId, frame := range diag.Frames {
		fFilePath, errs := writeFrame(frameId, frame, diagDir)
		filePaths = append(filePaths, fFilePath...)
		if len(errs) > 0 {
			// it would be nice if we could wrap this list of errors into something formal but this logs well
			frameErrors.Errors = append(frameErrors.Errors, fmt.Errorf("unable to write frame %s for %s", frameId, name))
			frameErrors.Errors = append(frameErrors.Errors, errs...)
		}
	}
	return filePaths, frameErrors
}

func writeFrame(frameId string, frame data.Frame, baseDir string) ([]string, []error) {
	switch f := frame.(type) {
	case data.DatabaseFrame:
		return writeDatabaseFrame(frameId, f, baseDir)
	case data.ConfigFileFrame:
		return writeConfigFrame(frameId, f, baseDir)
	case data.DirectoryFileFrame:
		return processDirectoryFileFrame(frameId, f, baseDir)
	case data.FileFrame:
		return processFileFrame(frameId, f, baseDir)
	case data.HierarchicalFrame:
		return writeHierarchicalFrame(frameId, f, baseDir)
	default:
		// for now our data frame writer supports all frames
		return writeDatabaseFrame(frameId, frame, baseDir)
	}
}

func writeHierarchicalFrame(frameId string, frame data.HierarchicalFrame, baseDir string) ([]string, []error) {
	filePaths, errs := writeFrame(frameId, frame.DataFrame, baseDir)
	for _, subFrame := range frame.SubFrames {
		subDir := filepath.Join(baseDir, subFrame.Name())
		if err := os.MkdirAll(subDir, os.ModePerm); err != nil {
			errs = append(errs, err)
			continue
		}
		subPaths, subErrs := writeFrame(subFrame.Name(), subFrame, subDir)
		filePaths = append(filePaths, subPaths...)
		errs = append(errs, subErrs...)
	}
	return filePaths, errs
}

func writeDatabaseFrame(frameId string, frame data.Frame, baseDir string) ([]string, []error) {
	frameFilePath := filepath.Join(baseDir, fmt.Sprintf("%s.csv", frameId))
	var errs []error
	f, err := os.Create(frameFilePath)
	if err != nil {
		errs = append(errs, errors.Wrapf(err, "unable to create directory for frame %s", frameId))
		return []string{}, errs
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write(frame.Columns()); err != nil {
		errs = append(errs, errors.Wrapf(err, "unable to write columns for frame %s", frameId))
		return []string{}, errs
	}
	// we don't collect an error for every line here like configs and logs - could mean a lot of unnecessary noise
	for {
		values, ok, err := frame.Next()
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "unable to read frame %s", frameId))
			return []string{}, errs
		}
		if !ok {
			return []string{frameFilePath}, errs
		}
		sValues := make([]string, len(values))
		for i, value := range values {
			sValues[i] = fmt.Sprintf("%v", value)
		}
		if err := w.Write(sValues); err != nil {
			errs = append(errs, errors.Wrapf(err, "unable to write row for frame %s", frameId))
			return []string{}, errs
		}
	}
}

func writeConfigFrame(frameId string, frame data.ConfigFileFrame, baseDir string) ([]string, []error) {
	var errs []error
	frameDirectory := filepath.Join(baseDir, frameId)
	if err := os.MkdirAll(frameDirectory, os.ModePerm); err != nil {
		errs = append(errs, errors.Wrapf(err, "unable to create directory for frame %s", frameId))
		return []string{}, errs
	}
	// this holds our files included
	includesDirectory := filepath.Join(frameDirectory, "includes")
	if err := os.MkdirAll(includesDirectory, os.ModePerm); err != nil {
		errs = append(errs, errors.Wrapf(err, "unable to create includes directory for frame %s", frameId))
		return []string{}, errs
	}
	for {
		values, ok, err := frame.Next()
		if err != nil {
			errs = append(errs, err)
			return []string{frameDirectory}, errs
		}
		if !ok {
			return []string{frameDirectory}, errs
		}
		configFile := values[0].(data.ConfigFile)
		if !configFile.IsIncluded() {
			relPath := strings.TrimPrefix(configFile.FilePath(), frame.Directory)
			destPath := path.Join(frameDirectory, relPath)
			if err = configFile.Copy(destPath, true); err != nil {
				errs = append(errs, errors.Wrapf(err, "Unable to copy file %s", configFile.FilePath()))
			}
		} else {
			// include files could be anywhere - potentially multiple with the same name. We thus, recreate the directory
			// hierarchy under includes to avoid collisions
			destPath := path.Join(includesDirectory, configFile.FilePath())
			if err = configFile.Copy(destPath, true); err != nil {
				errs = append(errs, errors.Wrapf(err, "Unable to copy file %s", configFile.FilePath()))
			}
		}

	}
}

func processDirectoryFileFrame(frameId string, frame data.DirectoryFileFrame, baseDir string) ([]string, []error) {
	var errs []error
	// each set of files goes under its own directory to preserve grouping
	frameDirectory := filepath.Join(baseDir, frameId)
	if err := os.MkdirAll(frameDirectory, os.ModePerm); err != nil {
		errs = append(errs, errors.Wrapf(err, "unable to create directory for frame %s", frameId))
		return []string{}, errs
	}
	for {
		values, ok, err := frame.Next()
		if err != nil {
			errs = append(errs, err)
			return []string{frameDirectory}, errs
		}
		if !ok {
			return []string{frameDirectory}, errs
		}
		file := values[0].(data.SimpleFile)
		relPath := strings.TrimPrefix(file.FilePath(), frame.Directory)
		destPath := path.Join(frameDirectory, relPath)

		if err = file.Copy(destPath, true); err != nil {
			errs = append(errs, errors.Wrapf(err, "unable to copy file %s for frame %s", file, frameId))
		}
	}
}

func processFileFrame(frameId string, frame data.FileFrame, baseDir string) ([]string, []error) {
	var errs []error
	frameDirectory := filepath.Join(baseDir, frameId)
	if err := os.MkdirAll(frameDirectory, os.ModePerm); err != nil {
		errs = append(errs, errors.Wrapf(err, "unable to create directory for frame %s", frameId))
		return []string{}, errs
	}
	for {
		values, ok, err := frame.Next()
		if err != nil {
			errs = append(errs, err)
		}
		if !ok {
			return []string{frameDirectory}, errs
		}
		file := values[0].(data.SimpleFile)
		// we need an absolute path to preserve the directory hierarchy
		dir, err := filepath.Abs(filepath.Dir(file.FilePath()))
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "unable to determine dir for %s", file.FilePath()))
		}
		outputDir := filepath.Join(frameDirectory, dir)
		if _, err := os.Stat(outputDir); os.IsNotExist(err) {
			if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
				errs = append(errs, errors.Wrapf(err, "unable to create directory for %s", file.FilePath()))
			} else {
				outputPath := filepath.Join(outputDir, filepath.Base(file.FilePath()))
				err = file.Copy(outputPath, false)
				if err != nil {
					errs = append(errs, errors.Wrapf(err, "unable to copy file %s", file.FilePath()))
				}
			}
		}
	}
}

func getWorkingDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		workingPath, err := os.Getwd()
		if err != nil {
			return "", err
		}
		return filepath.Join(workingPath, path), nil
	}
	return path, nil
}

func createArchiveMap(filePaths []string, prefix string) map[string]string {
	archiveMap := make(map[string]string)
	for _, path := range filePaths {
		archiveMap[path] = strings.TrimPrefix(path, prefix)
	}
	return archiveMap
}

func createArchive(outputFile string, filePaths map[string]string) error {
	files, err := archiver.FilesFromDisk(nil, filePaths)
	if err != nil {
		return err
	}
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	format := archiver.CompressedArchive{
		Compression: archiver.Gz{},
		Archival:    archiver.Tar{},
	}
	err = format.Archive(context.Background(), out, files)
	return err
}

func (o SimpleOutput) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value: "./",
				Param: config.NewParam("directory", "Directory in which to create dump. Defaults to the current directory.", false),
			},
			config.StringOptions{
				Value: "csv",
				// TODO: add tsv and others here later
				Options: []string{"csv"},
				Param:   config.NewParam("format", "Format of exported files", false),
			},
			config.BoolParam{
				Value: false,
				Param: config.NewParam("skip_archive", "Don't compress output to an archive", false),
			},
		},
	}
}

func (o SimpleOutput) Description() string {
	return "Writes out the diagnostic bundle as files in a structured directory, optionally producing a compressed archive."
}

// here we register the output for use
func init() {
	outputs.Register(OutputName, func() (outputs.Output, error) {
		return SimpleOutput{}, nil
	})
}
