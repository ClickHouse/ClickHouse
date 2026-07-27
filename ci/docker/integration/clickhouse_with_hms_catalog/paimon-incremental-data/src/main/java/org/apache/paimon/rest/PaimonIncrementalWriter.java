package org.apache.paimon.rest;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.List;

public class PaimonIncrementalWriter {
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            throw new RuntimeException(
                "Usage: <rootPath> <database> <table> <startId> <rowsPerCommit> <commitTimes>");
        }
        String rootPath = args[0];
        String database = args[1];
        String tableName = args[2];
        int startId = Integer.parseInt(args[3]);
        int rowsPerCommit = Integer.parseInt(args[4]);
        int commitTimes = Integer.parseInt(args[5]);

        insertDataMultiple(rootPath, database, tableName, startId, rowsPerCommit, commitTimes);
    }

    private static void insertDataMultiple(
        String rootPath,
        String database,
        String tableName,
        int startId,
        int rowsPerCommit,
        int commitTimes) throws Exception {
        CatalogContext context = CatalogContext.create(new Path(rootPath));
        Catalog catalog = org.apache.paimon.catalog.CatalogFactory.createCatalog(context);
        Identifier tableId = Identifier.create(database, tableName);
        ensureTableExists(catalog, tableId);
        Table table = catalog.getTable(tableId);

        int currentId = startId;
        for (int round = 0; round < commitTimes; ++round) {
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            TableWriteImpl writer = (TableWriteImpl) writeBuilder.newWrite()
                .withIOManager(new IOManagerImpl(rootPath));
            for (int i = 0; i < rowsPerCommit; i++) {
                InternalRow row = createRow(currentId++, table.rowType());
                writer.write(row);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            BatchTableCommit commit = writeBuilder.newCommit();
            commit.commit(messages);
        }
    }

    private static InternalRow createRow(int id, RowType rowType) {
        GenericRow row = new GenericRow(rowType.getFieldCount());
        List<DataType> fieldTypes = rowType.getFieldTypes();

        for (int i = 0; i < fieldTypes.size(); i++) {
            DataType fieldType = fieldTypes.get(i);
            switch (fieldType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    row.setField(i, BinaryString.fromString("VARCHAR_" + id));
                    break;
                case TINYINT:
                    row.setField(i, (byte)id);
                    break;
                case SMALLINT:
                    row.setField(i, (short)id);
                    break;
                case INTEGER:
                    row.setField(i, id);
                    break;
                case BIGINT:
                    row.setField(i, (long)id);
                    break;
                case FLOAT:
                    row.setField(i, (float)id + 0.1f);
                    break;
                case DOUBLE:
                    row.setField(i, (double)id + 0.1);
                    break;
                default:
                    throw new RuntimeException("Unsupported Type: " + fieldType);
            }
        }
        return row;
    }

    private static void ensureTableExists(Catalog catalog, Identifier tableId) throws Exception {
        try {
            catalog.createDatabase(tableId.getDatabaseName(), true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // already exists
        }

        Schema schema = Schema.newBuilder()
            .column("f_string", DataTypes.STRING())
            .column("f_int", DataTypes.INT())
            .column("f_bigint", DataTypes.BIGINT())
            .partitionKeys("f_string")
            .build();

        try {
            catalog.createTable(tableId, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // already exists
        }
    }
}
