package data

import (
	"bufio"
	"encoding/xml"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type File interface {
	Copy(destPath string, removeSensitive bool) error
	FilePath() string
}

type SimpleFile struct {
	Path string
}

// Copy supports removeSensitive for other file types but for a simple file this doesn't do anything
func (s SimpleFile) Copy(destPath string, removeSensitive bool) error {
	// simple copy easiest
	if err := utils.CopyFile(s.FilePath(), destPath); err != nil {
		return errors.Wrapf(err, "unable to copy file %s", s.FilePath())
	}
	return nil
}

func (s SimpleFile) FilePath() string {
	return s.Path
}

func NewFileFrame(name string, filePaths []string) FileFrame {
	i := 0
	files := make([]File, len(filePaths))
	for i, path := range filePaths {
		files[i] = SimpleFile{
			Path: path,
		}
	}
	return FileFrame{
		name:  name,
		i:     &i,
		files: files,
	}
}

type FileFrame struct {
	name  string
	i     *int
	files []File
}

func (f FileFrame) Next() ([]interface{}, bool, error) {
	if len(f.files) == *(f.i) {
		return nil, false, nil
	}
	file := f.files[*f.i]
	*f.i++
	value := make([]interface{}, 1)
	value[0] = file
	return value, true, nil
}

func (f FileFrame) Columns() []string {
	return []string{"files"}
}

func (f FileFrame) Name() string {
	return f.name
}

// DirectoryFileFrame represents a set of files under a directory
type DirectoryFileFrame struct {
	FileFrame
	Directory string
}

func NewFileDirectoryFrame(directory string, exts []string) (DirectoryFileFrame, []error) {
	filePaths, errs := utils.ListFilesInDirectory(directory, exts)
	files := make([]File, len(filePaths))
	for i, path := range filePaths {
		files[i] = SimpleFile{
			Path: path,
		}
	}
	i := 0
	return DirectoryFileFrame{
		Directory: directory,
		FileFrame: FileFrame{
			files: files,
			i:     &i,
		},
	}, errs
}

func (f DirectoryFileFrame) Next() ([]interface{}, bool, error) {
	if len(f.files) == *(f.i) {
		return nil, false, nil
	}
	file := f.files[*f.i]
	*f.i++
	value := make([]interface{}, 1)
	value[0] = file
	return value, true, nil
}

func (f DirectoryFileFrame) Columns() []string {
	return []string{"files"}
}

func (f DirectoryFileFrame) Name() string {
	return f.Directory
}

type ConfigFile interface {
	File
	FindLogPaths() ([]string, error)
	FindIncludedConfig() (ConfigFile, error)
	IsIncluded() bool
}

type ConfigFileFrame struct {
	i         *int
	Directory string
	files     []ConfigFile
}

func (f ConfigFileFrame) Next() ([]interface{}, bool, error) {
	if len(f.files) == *(f.i) {
		return nil, false, nil
	}
	file := f.files[*f.i]
	*f.i++
	value := make([]interface{}, 1)
	value[0] = file
	return value, true, nil
}

func (f ConfigFileFrame) Name() string {
	return f.Directory
}

func NewConfigFileFrame(directory string) (ConfigFileFrame, []error) {
	files, errs := utils.ListFilesInDirectory(directory, []string{"*.xml", "*.yaml", "*.yml"})
	// we can't predict the length because of include files
	var configs []ConfigFile

	for _, path := range files {
		var configFile ConfigFile
		switch ext := filepath.Ext(path); ext {
		case ".xml":
			configFile = XmlConfigFile{
				Path:     path,
				Included: false,
			}
		case ".yml":
			configFile = YamlConfigFile{
				Path:     path,
				Included: false,
			}
		case ".yaml":
			configFile = YamlConfigFile{
				Path: path,
			}
		}
		if configFile != nil {
			configs = append(configs, configFile)
			// add any included configs
			iConf, err := configFile.FindIncludedConfig()
			if err != nil {
				errs = append(errs, err)
			} else {
				if iConf.FilePath() != "" {
					configs = append(configs, iConf)
				}
			}
		}
	}
	i := 0

	return ConfigFileFrame{
		i:         &i,
		Directory: directory,
		files:     configs,
	}, errs
}

func (f ConfigFileFrame) Columns() []string {
	return []string{"config"}
}

func (f ConfigFileFrame) FindLogPaths() (logPaths []string, errors []error) {
	for _, configFile := range f.files {
		paths, err := configFile.FindLogPaths()
		if err != nil {
			errors = append(errors, err)
		} else {
			logPaths = append(logPaths, paths...)
		}
	}
	return logPaths, errors
}

type XmlConfigFile struct {
	Path     string
	Included bool
}

// these patterns will be used to remove sensitive content - matches of the pattern will be replaced with the key
var xmlSensitivePatterns = map[string]*regexp.Regexp{
	"<password>Replaced</password>":                       regexp.MustCompile(`<password>(.*)</password>`),
	"<password_sha256_hex>Replaced</password_sha256_hex>": regexp.MustCompile(`<password_sha256_hex>(.*)</password_sha256_hex>`),
	"<secret_access_key>Replaced</secret_access_key>":     regexp.MustCompile(`<secret_access_key>(.*)</secret_access_key>`),
	"<access_key_id>Replaced</access_key_id>":             regexp.MustCompile(`<access_key_id>(.*)</access_key_id>`),
	"<secret>Replaced</secret>":                           regexp.MustCompile(`<secret>(.*)</secret>`),
}

func (x XmlConfigFile) Copy(destPath string, removeSensitive bool) error {
	if !removeSensitive {
		// simple copy easiest
		if err := utils.CopyFile(x.FilePath(), destPath); err != nil {
			return errors.Wrapf(err, "unable to copy file %s", x.FilePath())
		}
		return nil
	}
	return sensitiveFileCopy(x.FilePath(), destPath, xmlSensitivePatterns)
}

func (x XmlConfigFile) FilePath() string {
	return x.Path
}

func (x XmlConfigFile) IsIncluded() bool {
	return x.Included
}

type XmlLoggerConfig struct {
	XMLName  xml.Name `xml:"logger"`
	ErrorLog string   `xml:"errorlog"`
	Log      string   `xml:"log"`
}

type YandexXMLConfig struct {
	XMLName     xml.Name        `xml:"yandex"`
	Clickhouse  XmlLoggerConfig `xml:"logger"`
	IncludeFrom string          `xml:"include_from"`
}

type XmlConfig struct {
	XMLName     xml.Name        `xml:"clickhouse"`
	Clickhouse  XmlLoggerConfig `xml:"logger"`
	IncludeFrom string          `xml:"include_from"`
}

func (x XmlConfigFile) UnmarshallConfig() (XmlConfig, error) {
	inputFile, err := ioutil.ReadFile(x.Path)

	if err != nil {
		return XmlConfig{}, err
	}
	var cConfig XmlConfig
	err = xml.Unmarshal(inputFile, &cConfig)
	if err == nil {
		return XmlConfig{
			Clickhouse:  cConfig.Clickhouse,
			IncludeFrom: cConfig.IncludeFrom,
		}, nil
	}
	// attempt to marshall as yandex file
	var yConfig YandexXMLConfig
	err = xml.Unmarshal(inputFile, &yConfig)
	if err != nil {
		return XmlConfig{}, err
	}
	return XmlConfig{
		Clickhouse:  yConfig.Clickhouse,
		IncludeFrom: yConfig.IncludeFrom,
	}, nil
}

func (x XmlConfigFile) FindLogPaths() ([]string, error) {
	var paths []string
	config, err := x.UnmarshallConfig()
	if err != nil {
		return nil, err
	}
	if config.Clickhouse.Log != "" {
		paths = append(paths, config.Clickhouse.Log)
	}
	if config.Clickhouse.ErrorLog != "" {
		paths = append(paths, config.Clickhouse.ErrorLog)
	}

	return paths, nil
}

func (x XmlConfigFile) FindIncludedConfig() (ConfigFile, error) {
	if x.Included {
		//can't recurse
		return XmlConfigFile{}, nil
	}
	config, err := x.UnmarshallConfig()
	if err != nil {
		return XmlConfigFile{}, err
	}
	// we need to convert this
	if config.IncludeFrom != "" {
		if filepath.IsAbs(config.IncludeFrom) {
			return XmlConfigFile{Path: config.IncludeFrom, Included: true}, nil
		}
		confDir := filepath.Dir(x.FilePath())
		return XmlConfigFile{Path: path.Join(confDir, config.IncludeFrom), Included: true}, nil
	}
	return XmlConfigFile{}, nil
}

type YamlConfigFile struct {
	Path     string
	Included bool
}

var ymlSensitivePatterns = map[string]*regexp.Regexp{
	"password: 'Replaced'":            regexp.MustCompile(`password:\s*.*$`),
	"password_sha256_hex: 'Replaced'": regexp.MustCompile(`password_sha256_hex:\s*.*$`),
	"access_key_id: 'Replaced'":       regexp.MustCompile(`access_key_id:\s*.*$`),
	"secret_access_key: 'Replaced'":   regexp.MustCompile(`secret_access_key:\s*.*$`),
	"secret: 'Replaced'":              regexp.MustCompile(`secret:\s*.*$`),
}

func (y YamlConfigFile) Copy(destPath string, removeSensitive bool) error {
	if !removeSensitive {
		// simple copy easiest
		if err := utils.CopyFile(y.FilePath(), destPath); err != nil {
			return errors.Wrapf(err, "unable to copy file %s", y.FilePath())
		}
		return nil
	}
	return sensitiveFileCopy(y.FilePath(), destPath, ymlSensitivePatterns)
}

func (y YamlConfigFile) FilePath() string {
	return y.Path
}

func (y YamlConfigFile) IsIncluded() bool {
	return y.Included
}

type YamlLoggerConfig struct {
	Log      string
	ErrorLog string
}

type YamlConfig struct {
	Logger       YamlLoggerConfig
	Include_From string
}

func (y YamlConfigFile) FindLogPaths() ([]string, error) {
	var paths []string
	inputFile, err := ioutil.ReadFile(y.Path)
	if err != nil {
		return nil, err
	}
	var config YamlConfig
	err = yaml.Unmarshal(inputFile, &config)
	if err != nil {
		return nil, err
	}
	if config.Logger.Log != "" {
		paths = append(paths, config.Logger.Log)
	}
	if config.Logger.ErrorLog != "" {
		paths = append(paths, config.Logger.ErrorLog)
	}
	return paths, nil
}

func (y YamlConfigFile) FindIncludedConfig() (ConfigFile, error) {
	if y.Included {
		//can't recurse
		return YamlConfigFile{}, nil
	}
	inputFile, err := ioutil.ReadFile(y.Path)
	if err != nil {
		return YamlConfigFile{}, err
	}
	var config YamlConfig
	err = yaml.Unmarshal(inputFile, &config)
	if err != nil {
		return YamlConfigFile{}, err
	}
	if config.Include_From != "" {
		if filepath.IsAbs(config.Include_From) {
			return YamlConfigFile{Path: config.Include_From, Included: true}, nil
		}
		confDir := filepath.Dir(y.FilePath())
		return YamlConfigFile{Path: path.Join(confDir, config.Include_From), Included: true}, nil
	}
	return YamlConfigFile{}, nil
}

func sensitiveFileCopy(sourcePath string, destPath string, patterns map[string]*regexp.Regexp) error {
	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "unable to create directory %s", destDir)
	}
	// currently, we don't unmarshall into a struct - we want to preserve structure and comments. Possibly could
	// be handled but for simplicity we do a line parse for now
	inputFile, err := os.Open(sourcePath)

	if err != nil {
		return err
	}
	defer inputFile.Close()
	outputFile, err := os.Create(destPath)

	if err != nil {
		return err
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan() {
		line := scanner.Text()
		for repl, pattern := range patterns {
			line = pattern.ReplaceAllString(line, repl)
		}
		_, err = writer.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	writer.Flush()
	return nil
}
