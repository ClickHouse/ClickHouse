package org.foo.clickhouse

import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.TimeStampSecVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.foo.arrow.ArrowSchemaTypes.boolField
import org.foo.arrow.ArrowSchemaTypes.int32Field
import org.foo.arrow.ArrowSchemaTypes.nullableDecimal7625Field
import org.foo.arrow.ArrowSchemaTypes.nullableStringField
import org.foo.arrow.ArrowSchemaTypes.nullableTimestampField
import org.foo.arrow.ArrowSchemaTypes.stringField
import org.foo.arrow.ArrowSchemaTypes.timestampField
import org.foo.arrow.ArrowSchemaTypes.uInt128Field
import org.foo.arrow.ArrowSerializer
import org.foo.arrow.ArrowSerializerUtils.setNullableSafe
import org.foo.arrow.ArrowVectors
import org.foo.clickhouse.ColumnName.*
import org.foo.domain.DiscoveryCall
import org.foo.domain.FlattenedGoatResult
import org.foo.domain.JobId
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Clock
import java.util.*

data object JobIdSchemaV3 {
    fun columnType() = "UInt128"
    fun schemaField() = uInt128Field(JobIdForUintColumn.value)
    fun vectorFor(root: VectorSchemaRoot): JobIdSchemaVector {
        val vectorFor = JobIdForUintColumn.vectorFor(root)
        return object : JobIdSchemaVector {
            override fun allocateNew(size: Int) {
                vectorFor.allocateNew(size)
            }

            override fun setSafe(index: Int, jobId: JobId) {
                vectorFor.setSafe(index, uint128ToByteArray(uuidToClickHouseUInt128(jobId.value)))
            }
        }
    }

    fun equalClause(jobId: JobId?): String = "toUInt128(toUUID(${jobId?.let { "'${it.asString()}'" } ?: "NULL"}))"

    private fun uint128ToByteArray(value: BigInteger): ByteArray {
        val bytes = value.toByteArray()
        val padded = ByteArray(16)
        val copyStart = 16 - bytes.size
        bytes.copyInto(padded, copyStart.coerceAtLeast(0), maxOf(0, -copyStart), bytes.size)
        return padded.reversedArray()
    }

    private fun uuidToClickHouseUInt128(uuid: UUID): BigInteger {
        val buffer = ByteBuffer.allocate(16)
        buffer.order(ByteOrder.BIG_ENDIAN)
        buffer.putLong(uuid.mostSignificantBits)
        buffer.putLong(uuid.leastSignificantBits)
        return BigInteger(1, buffer.array()) // Unsigned interpretation
    }
}

interface JobIdSchemaVector {
    fun allocateNew(size: Int)
    fun setSafe(index: Int, jobId: JobId)
}

class ResultsSchema(private val databaseName: String) {

    fun ddls(resultsTableName: ClickhouseTableName): List<String> =
        listOf(initialSchema(resultsTableName))

    private fun initialSchema(resultsTableName: ClickhouseTableName): String {
        return """CREATE TABLE IF NOT EXISTS $databaseName.${resultsTableName.value}
                    (
                        `${JobIdForUintColumn.value}` ${JobIdSchemaV3.columnType()},
                        `${C1.value}` DateTime,
                        `${C2.value}` LowCardinality(String),
                        `${C0.value}` UUID,
                        `${C6.value}` String,
                        `${C7.value}` Nullable(String),
                        `${C8.value}` LowCardinality(Nullable(String)),
                        `${C9.value}` LowCardinality(Nullable(String)),
                        `${C10.value}` Nullable(String),
                        `${C11.value}` Nullable(String),
                        `${C12.value}` LowCardinality(Nullable(String)),
                        `${C13.value}` Nullable(String),
                        `${C14.value}` Nullable(String),
                        `${C15.value}` LowCardinality(Nullable(String)),
                        `${C16.value}` LowCardinality(Nullable(String)),
                        `${C19.value}` Nullable(String),
                        `${C20.value}` LowCardinality(Nullable(String)),
                        `${C21.value}` Nullable(String),
                        `${C22.value}` Nullable(String),
                        `${C23.value}` Nullable(String),
                        `${C24.value}` Nullable(String),
                        `${C25.value}` Nullable(String),
                        `${C26.value}` Nullable(String),
                        `${C27.value}` Nullable(String),
                        `${C28.value}` Nullable(String),
                        `${C29.value}` LowCardinality(Nullable(String)),
                        `${C30.value}` LowCardinality(Nullable(String)),
                        `${C31.value}` LowCardinality(Nullable(String)),
                        `${C32.value}` Nullable(String),
                        `${C35.value}` Nullable(String),
                        `${C33.value}` Nullable(String),
                        `${C34.value}` LowCardinality(Nullable(String)),
                        `${C3.value}` Nullable(String),
                        `${C36.value}` Nullable(String),
                        `${C37.value}` LowCardinality(Nullable(String)),
                        `${C39.value}` Nullable(String),
                        `${C41.value}` Nullable(String),
                        `${C42.value}` LowCardinality(Nullable(String)),
                        `${C43.value}` LowCardinality(Nullable(String)),
                        `${C44.value}` LowCardinality(String),
                        `${C45.value}` LowCardinality(Nullable(String)),
                        `${C46.value}` Nullable(String),
                        `${C47.value}` Nullable(String),
                        `${C48.value}` LowCardinality(Nullable(String)),
                        `${C49.value}` Nullable(String),
                        `${C50.value}` Nullable(Decimal(76, 25)),
                        `${C51.value}` LowCardinality(Nullable(String)),
                        `${TTL.value}` Int32 DEFAULT 7,
                        `${C52.value}` Nullable(Decimal(76, 25)),
                        `${C53.value}` Nullable(Decimal(76, 25)),
                        `${C54.value}` Nullable(Decimal(76, 25)),
                        `${C55.value}` Nullable(String),
                        `${C57.value}` Nullable(String),
                        `${C58.value}` Nullable(Decimal(76, 25)),
                        `${C59.value}` Nullable(Decimal(76, 25)),
                        `${C60.value}` Nullable(String),
                        `${C61.value}` Nullable(Decimal(76, 25)),
                        `${C62.value}` Nullable(String),
                        `${C63.value}` Nullable(String),
                        `${C64.value}` Nullable(String),
                        `${C65.value}` LowCardinality(Nullable(String)),
                        `${C66.value}` Nullable(Decimal(76, 25)),
                        `${C68.value}` LowCardinality(Nullable(String)),
                        `${C69.value}` Nullable(String),
                        `${C70.value}` LowCardinality(Nullable(String)),
                        `${C71.value}` LowCardinality(Nullable(String)),
                        `${C72.value}` Nullable(String),
                        `${C73.value}` Nullable(Bool),
                        `${C4.value}` DateTime64(0),
                        `${C74.value}` Bool DEFAULT false,
                        `errors` Array(Tuple(String, String)) DEFAULT [],
                        `${C75.value}` Nullable(Decimal(76, 25)),
                        `${C76.value}` Nullable(Decimal(76, 25)),
                        `${C77.value}` Nullable(Decimal(76, 25)),
                        `${C78.value}` Nullable(Decimal(76, 25)),
                        `${C79.value}` Nullable(Decimal(76, 25)),
                        `${C80.value}` Nullable(Decimal(76, 25)),
                        `${C81.value}` Nullable(Decimal(76, 25)),
                        `${C82.value}` Nullable(Decimal(76, 25)),
                        `${C83.value}` Nullable(Decimal(76, 25)),
                        `${C84.value}` Bool DEFAULT false,
                        `${C85.value}` Nullable(Decimal(76, 25)),
                        `${C86.value}` Nullable(String),
                        `${C87.value}` Nullable(String),
                        `${C88.value}` Nullable(String),
                        `${C17.value}` LowCardinality(Nullable(String)),
                        `${C89.value}` Nullable(String),
                        `${C90.value}` LowCardinality(Nullable(String)),
                        `${C91.value}` Nullable(Decimal(76, 25)),
                        `${C92.value}` LowCardinality(Nullable(String)),
                        `${C40.value}` Nullable(String),
                        `skajdfhw_aksdf` Array(Tuple(String, Bool, Nullable(DateTime64(0)))) DEFAULT [],
                        `${C38.value}` Nullable(String),
                        `${C93.value}` Nullable(Decimal(76, 25)),
                        `${C94.value}` LowCardinality(Nullable(String)),
                        `${C95.value}` Nullable(String),
                        `${C97.value}` Nullable(String),
                        `${C56.value}` Nullable(Decimal(76, 25)),
                        `${C98.value}` Bool DEFAULT false,
                        `${C18.value}` LowCardinality(Nullable(String)),
                        `${C99.value}` Nullable(Decimal(76, 25)),
                        `${C100.value}` Nullable(Decimal(76, 25)),
                        `${C96.value}` Nullable(String),
                        `${C67.value}` Nullable(Decimal(76, 25)),
                        `${PartitionColumn.value}` LowCardinality(String),
                        `${C101.value}` Nullable(String),
                        `${C102.value}` LowCardinality(Nullable(String)),
                        `${C103.value}` LowCardinality(Nullable(String)),
                        `${C104.value}` LowCardinality(Nullable(String)),
                        PROJECTION proj_job_results_pivot_v1
                        (
                            SELECT 
                                ${JobIdForUintColumn.value},
                                ${C44.value},
                                ${C45.value},
                                ${C47.value},
                                ${C63.value},
                                ${C64.value}
                            GROUP BY 
                                ${JobIdForUintColumn.value},
                                ${C44.value},
                                ${C45.value},
                                ${C47.value},
                                ${C63.value},
                                ${C64.value}
                        )
                    )
                    ENGINE = MergeTree()
                    PARTITION BY ${PartitionColumn.value}
                    ORDER BY (${JobIdForUintColumn.value}, ${C44.value})
                    TTL ${C1.value} + toIntervalDay(${TTL.value})
                    SETTINGS index_granularity = 16384, cache_populated_by_fetch = 1, index_granularity_bytes = 20971520
                    ;"""
    }
}

class SchemaProvider private constructor(val schema: Schema) {
    companion object {
        private val discoveryCallsStructFields = listOf(
            stringField("skajdfhw_sc"),
            boolField("sj_eidsjd_sud"),
            nullableTimestampField("osid")
        )
        private val discoveryCallsStruct = Field(
            "element",
            FieldType.nullable(ArrowType.Struct()),
            discoveryCallsStructFields
        )
        private val discoveryCallsField = Field(
            "skajdfhw_aksdf",
            FieldType.nullable(ArrowType.List()),
            listOf(discoveryCallsStruct)
        )

        private val errorsStructFields = listOf(
            nullableStringField("alias"),
            nullableStringField("message"),
        )
        private val errorsStruct = Field(
            "item",
            FieldType.nullable(ArrowType.Struct()),
            errorsStructFields
        )
        private val errorsField = Field(
            "errors",
            FieldType.nullable(ArrowType.List()),
            listOf(errorsStruct)
        )

        fun buildSchema(): Schema {
            return Schema(
                listOf(
                    JobIdSchemaV3.schemaField(),
                    stringField(C2.value),
                    timestampField(C1.value),
                    stringField(C0.value),
                    stringField(C6.value),
                    nullableStringField(C7.value),
                    nullableStringField(C8.value),
                    nullableStringField(C9.value),
                    nullableStringField(C10.value),
                    nullableStringField(C11.value),
                    nullableStringField(C12.value),
                    nullableStringField(C13.value),
                    nullableStringField(C14.value),
                    nullableStringField(C15.value),
                    nullableStringField(C16.value),
                    nullableStringField(C17.value),
                    nullableStringField(C18.value),
                    nullableStringField(C19.value),
                    nullableStringField(C20.value),
                    nullableStringField(C21.value),
                    nullableStringField(C22.value),
                    nullableStringField(C23.value),
                    nullableStringField(C24.value),
                    nullableStringField(C25.value),
                    nullableStringField(C26.value),
                    nullableStringField(C27.value),
                    nullableStringField(C28.value),
                    nullableStringField(C29.value),
                    nullableStringField(C30.value),
                    nullableStringField(C31.value),
                    nullableStringField(C32.value),
                    nullableStringField(C35.value),
                    nullableStringField(C33.value),
                    nullableStringField(C34.value),
                    nullableStringField(C3.value),
                    nullableStringField(C36.value),
                    nullableStringField(C37.value),
                    discoveryCallsField,
                    nullableStringField(C39.value),
                    nullableStringField(C38.value),
                    nullableStringField(C40.value),
                    nullableStringField(C41.value),
                    nullableStringField(C42.value),
                    nullableStringField(C43.value),
                    stringField(C44.value),
                    nullableStringField(C45.value),
                    nullableStringField(C46.value),
                    nullableStringField(C47.value),
                    nullableStringField(C48.value),
                    nullableStringField(C49.value),
                    nullableDecimal7625Field(C50.value),
                    nullableStringField(C51.value),
                    int32Field(TTL.value),
                    nullableDecimal7625Field(C52.value),
                    nullableDecimal7625Field(C53.value),
                    nullableDecimal7625Field(C54.value),
                    nullableStringField(C55.value),
                    nullableStringField(C57.value),
                    nullableDecimal7625Field(C58.value),
                    nullableDecimal7625Field(C59.value),
                    nullableStringField(C60.value),
                    nullableDecimal7625Field(C61.value),
                    nullableStringField(C62.value),
                    nullableStringField(C63.value),
                    nullableStringField(C64.value),
                    nullableStringField(C65.value),
                    nullableDecimal7625Field(C66.value),
                    nullableDecimal7625Field(C67.value),
                    nullableStringField(C68.value),
                    nullableStringField(C69.value),
                    nullableStringField(C70.value),
                    nullableStringField(C71.value),
                    nullableStringField(C72.value),
                    boolField(C73.value),
                    nullableTimestampField(C4.value),
                    boolField(C74.value),
                    errorsField,
                    nullableDecimal7625Field(C75.value),
                    nullableDecimal7625Field(C76.value),
                    nullableDecimal7625Field(C77.value),
                    nullableDecimal7625Field(C78.value),
                    nullableDecimal7625Field(C79.value),
                    nullableDecimal7625Field(C80.value),
                    nullableDecimal7625Field(C81.value),
                    nullableDecimal7625Field(C82.value),
                    nullableDecimal7625Field(C83.value),
                    boolField(C84.value),
                    nullableDecimal7625Field(C85.value),
                    nullableStringField(C86.value),
                    nullableStringField(C87.value),
                    nullableStringField(C88.value),
                    nullableStringField(C89.value),
                    nullableStringField(C90.value),
                    nullableDecimal7625Field(C91.value),
                    nullableStringField(C92.value),
                    nullableDecimal7625Field(C93.value),
                    nullableStringField(C94.value),
                    nullableStringField(C95.value),
                    nullableStringField(C96.value),
                    nullableStringField(C97.value),
                    nullableDecimal7625Field(C56.value),
                    boolField(C98.value),
                    nullableDecimal7625Field(C100.value),
                    nullableDecimal7625Field(C99.value),
                    stringField(PartitionColumn.value),
                    nullableStringField(C101.value),
                    nullableStringField(C102.value),
                    nullableStringField(C103.value),
                    nullableStringField(C104.value),
                )
            )
        }
    }
}


class ResultsVectors(root: VectorSchemaRoot) : ArrowVectors() {

    val jobIdSchemaVector = JobIdSchemaV3.vectorFor(root)
    val c2Vector: VarCharVector = C2.vectorFor(root)
    val c1Vector = C1.vectorFor(root)
    val c0Vector = C0.vectorFor(root)
    val c6Vector = C6.vectorFor(root)
    val c7Vector = C7.vectorFor(root)
    val c8Vector = C8.vectorFor(root)
    val c9Vector = C9.vectorFor(root)
    val c10Vector = C10.vectorFor(root)
    val c11Vector = C11.vectorFor(root)
    val c12Vector = C12.vectorFor(root)
    val c13Vector = C13.vectorFor(root)
    val c14Vector = C14.vectorFor(root)
    val c15Vector = C15.vectorFor(root)
    val c16Vector = C16.vectorFor(root)
    val c17Vector = C17.vectorFor(root)
    val c18Vector = C18.vectorFor(root)
    val c19Vector = C19.vectorFor(root)
    val c20Vector = C20.vectorFor(root)
    val c21Vector = C21.vectorFor(root)
    val c22Vector = C22.vectorFor(root)
    val c23Vector = C23.vectorFor(root)
    val c24Vector = C24.vectorFor(root)
    val c25Vector = C25.vectorFor(root)
    val c26Vector = C26.vectorFor(root)
    val c27Vector = C27.vectorFor(root)
    val c28Vector = C28.vectorFor(root)
    val c29Vector = C29.vectorFor(root)
    val c30Vector = C30.vectorFor(root)
    val c31Vector = C31.vectorFor(root)
    val c32Vector = C32.vectorFor(root)
    val c35Vector = C35.vectorFor(root)
    val c33Vector = C33.vectorFor(root)
    val c34Vector = C34.vectorFor(root)
    val c3Vector = C3.vectorFor(root)
    val c36Vector = C36.vectorFor(root)
    val c37Vector = C37.vectorFor(root)
    val nested1Vector = root.getVector("skajdfhw_aksdf") as ListVector
    val nested1ElementsVector = nested1Vector.dataVector as StructVector
    val nested1C1Vector = nested1ElementsVector.getChild("skajdfhw_sc") as VarCharVector
    val nested1C2Vector = nested1ElementsVector.getChild("sj_eidsjd_sud") as BitVector
    val nested1C3Vector = nested1ElementsVector.getChild("osid") as TimeStampSecVector
    val c39Vector = C39.vectorFor(root)
    val c38Vector = C38.vectorFor(root)
    val c40Vector = C40.vectorFor(root)
    val c41Vector = C41.vectorFor(root)
    val c42Vector = C42.vectorFor(root)
    val c43Vector = C43.vectorFor(root)
    val c44Vector = C44.vectorFor(root)
    val c45Vector = C45.vectorFor(root)
    val c46Vector = C46.vectorFor(root)
    val c47Vector = C47.vectorFor(root)
    val c48Vector = C48.vectorFor(root)
    val c49Vector = C49.vectorFor(root)
    val c50Vector = C50.vectorFor(root)
    val c51Vector = C51.vectorFor(root)
    val ttlVector = TTL.vectorFor(root)
    val c52Vector = C52.vectorFor(root)
    val c53Vector = C53.vectorFor(root)
    val c54Vector = C54.vectorFor(root)
    val c55Vector = C55.vectorFor(root)
    val c57Vector = C57.vectorFor(root)
    val c58Vector = C58.vectorFor(root)
    val c59Vector = C59.vectorFor(root)
    val c60Vector = C60.vectorFor(root)
    val c61Vector = C61.vectorFor(root)
    val c62Vector = C62.vectorFor(root)
    val c63Vector = C63.vectorFor(root)
    val c64Vector = C64.vectorFor(root)
    val c65Vector = C65.vectorFor(root)
    val c66Vector = C66.vectorFor(root)
    val c67Vector = C67.vectorFor(root)
    val c68Vector = C68.vectorFor(root)
    val c69Vector = C69.vectorFor(root)
    val c70Vector = C70.vectorFor(root)
    val c71Vector = C71.vectorFor(root)
    val c72Vector = C72.vectorFor(root)
    val c73Vector = C73.vectorFor(root)
    val c98Vector = C98.vectorFor(root)
    val c4Vector = C4.vectorFor(root)
    val c74Vector = C74.vectorFor(root)
    val c75Vector = C75.vectorFor(root)
    val c76Vector = C76.vectorFor(root)
    val c77Vector = C77.vectorFor(root)
    val c78Vector = C78.vectorFor(root)
    val c79Vector = C79.vectorFor(root)
    val c80Vector = C80.vectorFor(root)
    val c81Vector = C81.vectorFor(root)
    val c82Vector = C82.vectorFor(root)
    val c83Vector = C83.vectorFor(root)
    val c84Vector = C84.vectorFor(root)
    val errorsVector = root.getVector("errors") as ListVector
    val errorItemsVector = errorsVector.dataVector as StructVector
    val aliasVector = errorItemsVector.getChild("alias") as VarCharVector
    val errMsg = errorItemsVector.getChild("message") as VarCharVector
    val c85Vector = C85.vectorFor(root)
    val c86Vector = C86.vectorFor(root)
    val c87Vector = C87.vectorFor(root)
    val c88Vector = C88.vectorFor(root)
    val c89Vector = C89.vectorFor(root)
    val c90Vector = C90.vectorFor(root)
    val c91Vector = C91.vectorFor(root)
    val c92Vector = C92.vectorFor(root)
    val c93Vector = C93.vectorFor(root)
    val c94Vector = C94.vectorFor(root)
    val c95Vector = C95.vectorFor(root)
    val c96Vector = C96.vectorFor(root)
    val c97Vector = C97.vectorFor(root)
    val c56Vector = C56.vectorFor(root)
    val c99Vector = C99.vectorFor(root)
    val c100Vector = C100.vectorFor(root)
    val partitionVector = PartitionColumn.vectorFor(root)
    val c101Vector = C101.vectorFor(root)
    val c102Vector = C102.vectorFor(root)
    val c103Vector = C103.vectorFor(root)
    val c104Vector = C104.vectorFor(root)

    override fun allocateNew(size: Int) {
        jobIdSchemaVector.allocateNew(size)
        c2Vector.allocateNew(size)
        c1Vector.allocateNew(size)
        c0Vector.allocateNew(size)
        c6Vector.allocateNew(size)
        c7Vector.allocateNew(size)
        c8Vector.allocateNew(size)
        c9Vector.allocateNew(size)
        c10Vector.allocateNew(size)
        c11Vector.allocateNew(size)
        c12Vector.allocateNew(size)
        c13Vector.allocateNew(size)
        c14Vector.allocateNew(size)
        c15Vector.allocateNew(size)
        c16Vector.allocateNew(size)
        c17Vector.allocateNew(size)
        c18Vector.allocateNew(size)
        c19Vector.allocateNew(size)
        c20Vector.allocateNew(size)
        c21Vector.allocateNew(size)
        c22Vector.allocateNew(size)
        c23Vector.allocateNew(size)
        c24Vector.allocateNew(size)
        c25Vector.allocateNew(size)
        c26Vector.allocateNew(size)
        c27Vector.allocateNew(size)
        c28Vector.allocateNew(size)
        c29Vector.allocateNew(size)
        c30Vector.allocateNew(size)
        c31Vector.allocateNew(size)
        c32Vector.allocateNew(size)
        c35Vector.allocateNew(size)
        c33Vector.allocateNew(size)
        c34Vector.allocateNew(size)
        c3Vector.allocateNew(size)
        c36Vector.allocateNew(size)
        c37Vector.allocateNew(size)
        nested1Vector.allocateNew()
        nested1ElementsVector.allocateNew()
        nested1C1Vector.allocateNew(size)
        nested1C2Vector.allocateNew(size)
        nested1C3Vector.allocateNew(size)
        c39Vector.allocateNew(size)
        c38Vector.allocateNew(size)
        c41Vector.allocateNew(size)
        c42Vector.allocateNew(size)
        c43Vector.allocateNew(size)
        c44Vector.allocateNew(size)
        c45Vector.allocateNew(size)
        c46Vector.allocateNew(size)
        c47Vector.allocateNew(size)
        c48Vector.allocateNew(size)
        c49Vector.allocateNew(size)
        c50Vector.allocateNew(size)
        c51Vector.allocateNew(size)
        ttlVector.allocateNew(size)
        c52Vector.allocateNew(size)
        c53Vector.allocateNew(size)
        c54Vector.allocateNew(size)
        c55Vector.allocateNew(size)
        c57Vector.allocateNew(size)
        c58Vector.allocateNew(size)
        c59Vector.allocateNew(size)
        c60Vector.allocateNew(size)
        c61Vector.allocateNew(size)
        c62Vector.allocateNew(size)
        c63Vector.allocateNew(size)
        c64Vector.allocateNew(size)
        c65Vector.allocateNew(size)
        c66Vector.allocateNew(size)
        c67Vector.allocateNew(size)
        c68Vector.allocateNew(size)
        c69Vector.allocateNew(size)
        c70Vector.allocateNew(size)
        c71Vector.allocateNew(size)
        c72Vector.allocateNew(size)
        c73Vector.allocateNew(size)
        c4Vector.allocateNew(size)
        c74Vector.allocateNew(size)
        c75Vector.allocateNew(size)
        c76Vector.allocateNew(size)
        c77Vector.allocateNew(size)
        c78Vector.allocateNew(size)
        c79Vector.allocateNew(size)
        c80Vector.allocateNew(size)
        c81Vector.allocateNew(size)
        c82Vector.allocateNew(size)
        c83Vector.allocateNew(size)
        c84Vector.allocateNew(size)
        errorsVector.allocateNew()
        errorItemsVector.allocateNew()
        aliasVector.allocateNew(size)
        errMsg.allocateNew(size)
        c85Vector.allocateNew(size)
        c86Vector.allocateNew(size)
        c87Vector.allocateNew(size)
        c88Vector.allocateNew(size)
        c89Vector.allocateNew(size)
        c90Vector.allocateNew(size)
        c91Vector.allocateNew(size)
        c92Vector.allocateNew(size)
        c93Vector.allocateNew(size)
        c94Vector.allocateNew(size)
        c95Vector.allocateNew(size)
        c96Vector.allocateNew(size)
        c97Vector.allocateNew(size)
        c56Vector.allocateNew(size)
        c98Vector.allocateNew(size)
        c100Vector.allocateNew(size)
        c99Vector.allocateNew(size)
        partitionVector.allocateNew(size)
        c101Vector.allocateNew(size)
        c102Vector.allocateNew(size)
        c103Vector.allocateNew(size)
        c104Vector.allocateNew(size)
    }

}

class ResultsArrowSerializer(
    private val jobId: JobId,
    private val clock: Clock,
    override val schema: Schema
) : ArrowSerializer<FlattenedGoatResult> {

    override fun initVectors(root: VectorSchemaRoot): ArrowVectors = ResultsVectors(root)

    override fun populate(item: FlattenedGoatResult, vectors: ArrowVectors, index: Int) {
        vectors as ResultsVectors
        vectors.jobIdSchemaVector.setSafe(index, jobId)
        vectors.c2Vector.setSafe(index, item.c2.toByteArray())
        vectors.c1Vector.setSafe(index, clock.instant().epochSecond)
        vectors.c0Vector.setNullableSafe(index, item.c0)
        vectors.c6Vector.setNullableSafe(index, item.c6)
        vectors.c7Vector.setNullableSafe(index, item.c7)
        vectors.c8Vector.setNullableSafe(index, item.c8)
        vectors.c9Vector.setNullableSafe(index, item.c9)
        vectors.c10Vector.setNullableSafe(index, item.c10)
        vectors.c11Vector.setNullableSafe(index, item.c11)
        vectors.c12Vector.setNullableSafe(index, item.c12)
        vectors.c13Vector.setNullableSafe(index, item.c13)
        vectors.c14Vector.setNullableSafe(index, item.c14)
        vectors.c15Vector.setNullableSafe(index, item.c15)
        vectors.c16Vector.setNullableSafe(index, item.c16)
        vectors.c17Vector.setNullableSafe(index, item.c17)
        vectors.c18Vector.setNullableSafe(index, item.c18)
        vectors.c19Vector.setNullableSafe(index, item.c19)
        vectors.c20Vector.setNullableSafe(index, item.c20)
        vectors.c21Vector.setNullableSafe(index, item.c21)
        vectors.c22Vector.setNullableSafe(index, item.c22)
        vectors.c23Vector.setNullableSafe(index, item.c23)
        vectors.c24Vector.setNullableSafe(index, item.c24)
        vectors.c25Vector.setNullableSafe(index, item.c25)
        vectors.c26Vector.setNullableSafe(index, item.c26)
        vectors.c27Vector.setNullableSafe(index, item.c27)
        vectors.c28Vector.setNullableSafe(index, item.c28)
        vectors.c29Vector.setNullableSafe(index, item.c29)
        vectors.c30Vector.setNullableSafe(index, item.c30)
        vectors.c31Vector.setNullableSafe(index, item.c31)
        vectors.c32Vector.setNullableSafe(index, item.c32)
        vectors.c35Vector.setNullableSafe(index, item.c35)
        vectors.c33Vector.setNullableSafe(index, item.c33)
        vectors.c34Vector.setNullableSafe(index, item.c34)
        vectors.c3Vector.setNullableSafe(index, item.c3)
        vectors.c36Vector.setNullableSafe(index, item.c36)
        vectors.c37Vector.setNullableSafe(index, item.c37)

        vectors.nested1Vector.writer.apply {
            position = index
            startList()
            item.discoveryCalls?.forEach { discoveryCall ->
                struct().apply {
                    start()
                    varChar("skajdfhw_sc").writeVarChar(discoveryCall.dictionaryId)
                    when (discoveryCall) {
                        is DiscoveryCall.Hit -> {
                            bit("sj_eidsjd_sud").writeBit(1)
                            timeStampSec("osid").writeTimeStampSec(discoveryCall.date.epochSecond)
                        }

                        is DiscoveryCall.Miss -> {
                            bit("sj_eidsjd_sud").writeBit(0)
                        }
                    }
                    end()
                }
            }
            endList()
        }

        vectors.c39Vector.setNullableSafe(index, item.c38)
        vectors.c38Vector.setNullableSafe(index, item.c38)
        vectors.c40Vector.setNullableSafe(index, item.c40)
        vectors.c41Vector.setNullableSafe(index, item.c41)
        vectors.c42Vector.setNullableSafe(index, item.c42)
        vectors.c43Vector.setNullableSafe(index, item.c43)
        vectors.c44Vector.setSafe(index, item.c44.toByteArray())
        vectors.c45Vector.setNullableSafe(index, item.c45)
        vectors.c46Vector.setNullableSafe(index, item.c46)
        vectors.c47Vector.setNullableSafe(index, item.c47)
        vectors.c48Vector.setNullableSafe(index, item.c48)
        vectors.c49Vector.setNullableSafe(index, item.c49)
        vectors.c50Vector.setNullableSafe(index, item.c50)
        vectors.c51Vector.setNullableSafe(index, item.c51)
        vectors.ttlVector.setSafe(index, item.ttlDays)
        vectors.c52Vector.setNullableSafe(index, item.c52)
        vectors.c53Vector.setNullableSafe(index, item.c53)
        vectors.c54Vector.setNullableSafe(index, item.c54)
        vectors.c55Vector.setNullableSafe(index, item.c55)
        vectors.c57Vector.setNullableSafe(index, item.c57)
        vectors.c58Vector.setNullableSafe(index, item.c58)
        vectors.c59Vector.setNullableSafe(index, item.c59)
        vectors.c60Vector.setNullableSafe(index, item.c60)
        vectors.c61Vector.setNullableSafe(index, item.c61)
        vectors.c62Vector.setNullableSafe(index, item.c62)
        vectors.c63Vector.setNullableSafe(index, item.c63)
        vectors.c64Vector.setNullableSafe(index, item.c64)
        vectors.c65Vector.setNullableSafe(index, item.c65)
        vectors.c66Vector.setNullableSafe(index, item.c66)
        vectors.c67Vector.setNullableSafe(index, item.c67)
        vectors.c68Vector.setNullableSafe(index, item.c68)
        vectors.c69Vector.setNullableSafe(index, item.c69)
        vectors.c70Vector.setNullableSafe(index, item.c70)
        vectors.c71Vector.setNullableSafe(index, item.c71)
        vectors.c72Vector.setNullableSafe(index, item.c72)
        vectors.c73Vector.setNullableSafe(index, item.c73)
        vectors.c98Vector.setNullableSafe(index, item.c98)
        vectors.c4Vector.setSafe(index, item.c4.epochSecond)
        vectors.c74Vector.setNullableSafe(index, item.c74)
        vectors.c75Vector.setNullableSafe(index, item.c75)
        vectors.c76Vector.setNullableSafe(index, item.c76)
        vectors.c77Vector.setNullableSafe(index, item.c77)
        vectors.c78Vector.setNullableSafe(index, item.c78)
        vectors.c79Vector.setNullableSafe(index, item.c79)
        vectors.c80Vector.setNullableSafe(index, item.c80)
        vectors.c81Vector.setNullableSafe(index, item.c81)
        vectors.c82Vector.setNullableSafe(index, item.c82)
        vectors.c83Vector.setNullableSafe(index, item.c83)
        vectors.c84Vector.setNullableSafe(index, item.c84)

        vectors.errorsVector.startNewValue(index)
        item.errors.forEachIndexed { errorIndex, resultError ->
            vectors.aliasVector.setNullableSafe(errorIndex, resultError.resultAlias)
            vectors.errMsg.setNullableSafe(errorIndex, resultError.errorMessage)
        }
        vectors.errorsVector.endValue(index, item.errors.size)

        vectors.c85Vector.setNullableSafe(index, item.c85)
        vectors.c86Vector.setNullableSafe(index, item.c86)
        vectors.c87Vector.setNullableSafe(index, item.c87)
        vectors.c88Vector.setNullableSafe(index, item.c88)
        vectors.c89Vector.setNullableSafe(index, item.c89)
        vectors.c90Vector.setNullableSafe(index, item.c90)
        vectors.c91Vector.setNullableSafe(index, item.c91)
        vectors.c92Vector.setNullableSafe(index, item.c92)
        vectors.c93Vector.setNullableSafe(index, item.c93)
        vectors.c94Vector.setNullableSafe(index, item.c94)
        vectors.c95Vector.setNullableSafe(index, item.c95)
        vectors.c96Vector.setNullableSafe(index, item.c96)
        vectors.c97Vector.setNullableSafe(index, item.c97)
        vectors.c56Vector.setNullableSafe(index, item.c56)
        vectors.c99Vector.setNullableSafe(index, item.c99)
        vectors.c100Vector.setNullableSafe(index, item.c100)
        vectors.partitionVector.setSafe(index, item.partition.toByteArray())
        vectors.c101Vector.setNullableSafe(index, item.c101)
        vectors.c102Vector.setNullableSafe(index, item.c102)
        vectors.c103Vector.setNullableSafe(index, item.c103)
        vectors.c104Vector.setNullableSafe(index, item.c104)
    }
}
