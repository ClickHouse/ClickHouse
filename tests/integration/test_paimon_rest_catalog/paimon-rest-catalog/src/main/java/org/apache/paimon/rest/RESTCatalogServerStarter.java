package org.apache.paimon.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.*;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.auth.*;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

public class RESTCatalogServerStarter {
    public static void main(String[] args) throws Exception {
        String cmd_type = args[0];
        if (cmd_type.equals("server")) {
            String path = args[1];
            String tokenType = args[2];
            String host = args[3];
            String port = args[4];
            AuthProvider authProvider = createAuthProvider(tokenType);
            Map<String,String> token_map = Map.of(
                    "fs.oss.accessKeyId", "accessKeyId",
                    "fs.oss.accessKeySecret", "accessKeySecret",
                    "fs.oss.securityToken", "securityToken"
            );
            DataTokenStore.putDataToken("restWarehouse", "test.test_table", new RESTToken(
                    token_map, 0
            ));

            ConfigResponse config =
                    new ConfigResponse(
                            ImmutableMap.of(
                                    RESTCatalogInternalOptions.PREFIX.key(),
                                    "paimon",
                                    CatalogOptions.WAREHOUSE.key(),
                                    path),
                            ImmutableMap.of());
            UserDefinedRESTCatalogServer server = new UserDefinedRESTCatalogServer(path, authProvider, config, "restWarehouse");
            server.start(host, Integer.parseInt(port));
            System.out.println(server.getUrl());
        } else if (cmd_type.equals("insert")) {
            String path = args[1];
            String database = args[2];
            String tableName = args[3];
            insertData(path, database, tableName);
        }
        else
        {
            throw new RuntimeException("Unknown cmd type: " + cmd_type);
        }
    }

    private static AuthProvider createAuthProvider(String tokenType) {
        if (tokenType.equals("bearer")) {
            return new BearTokenAuthProvider("bearer-token-xxx-xxx-xxx");
        } else if (tokenType.equals("dlf")) {
            return DLFAuthProvider.buildAKToken("accessKeyId", "accessKeySecret", "", "cn-hangzhou");
        }
        throw new RuntimeException("Unknown token type");
    }

    private static void insertData(String rootPath, String database, String tableName) throws Exception {
        CatalogContext context = CatalogContext.create(new Path(rootPath));
        Catalog catalog = org.apache.paimon.catalog.CatalogFactory.createCatalog(context);
        Identifier tableId = Identifier.create(database, tableName);
        Table table = catalog.getTable(tableId);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        TableWriteImpl writer = (TableWriteImpl) writeBuilder.newWrite()
                .withIOManager(new IOManagerImpl(rootPath));
        for (int i = 0; i < 10; i++) {
            InternalRow row = createRow(i, table.rowType());
            writer.write(row);
        }
        List<CommitMessage> messages = writer.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
    }

    private static InternalRow createRow(int id, RowType rowType) {
        GenericRow row = new GenericRow(rowType.getFieldCount());
        List<DataType> fieldTypes = rowType.getFieldTypes();

        for (int i = 0; i < fieldTypes.size(); i++) {
            DataType fieldType = fieldTypes.get(i);
            switch (fieldType.getTypeRoot()) {
                case VARCHAR:
                    row.setField(i, BinaryString.fromString("VARCHAR"));
                    break;
                case TINYINT:
                    row.setField(i, (byte)1);
                    break;
                case SMALLINT:
                    row.setField(i, (short)1);
                    break;
                case INTEGER:
                    row.setField(i, 1);
                    break;
                case BIGINT:
                    row.setField(i, (long)1);
                    break;
                case FLOAT:
                    row.setField(i, (float)0.1);
                    break;
                case DOUBLE:
                    row.setField(i, (double)0.1);
                    break;
                default:
                    throw new RuntimeException("Unsupported Type: "+fieldType);
            }
        }
        return row;
    }
}
