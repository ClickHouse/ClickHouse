package platform

import (
	"errors"
	"sync"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/database"
)

var once sync.Once
var dbInit sync.Once

// manages all resources that collectors and outputs may wish to ensure inc. db connections

type DBClient interface {
	ReadTableNamesForDatabase(databaseName string) ([]string, error)
	ReadTable(databaseName string, tableName string, excludeColumns []string, orderBy data.OrderBy, limit int64) (data.Frame, error)
	ExecuteStatement(id string, statement string) (data.Frame, error)
	Version() (string, error)
}

var manager *ResourceManager

type ResourceManager struct {
	DbClient DBClient
}

func GetResourceManager() *ResourceManager {
	once.Do(func() {
		manager = &ResourceManager{}
	})
	return manager
}

func (m *ResourceManager) Connect(host string, port uint16, username string, password string) error {
	var err error
	var clientInstance DBClient
	init := false
	dbInit.Do(func() {
		clientInstance, err = database.NewNativeClient(host, port, username, password)
		manager.DbClient = clientInstance
		init = true
	})
	if !init {
		return errors.New("connect can only be called once")
	}
	return err
}
