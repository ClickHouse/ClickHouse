package org.foo.arrow

import org.apache.arrow.vector.types.TimeUnit.SECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

object ArrowSchemaTypes {
    private val DECIMAL_76_25 = ArrowType.Decimal(76, 25, 256)

    fun uInt128Field(name: String) = Field(name, FieldType.notNullable(ArrowType.FixedSizeBinary(16)), null)
    fun stringField(name: String) = Field(name, FieldType.notNullable(ArrowType.Utf8()), null)
    fun nullableStringField(name: String) = Field(name, FieldType.nullable(ArrowType.Utf8()), null)
    fun boolField(name: String) = Field(name, FieldType.nullable(ArrowType.Bool()), null)
    fun int32Field(name: String) = Field(name, FieldType.nullable(ArrowType.Int(32, true)), null)
    fun nullableDecimal7625Field(name: String) = Field(name, FieldType.nullable(DECIMAL_76_25), null)
    fun timestampField(name: String) = Field(name, FieldType.notNullable(ArrowType.Timestamp(SECOND, null)), null)
    fun nullableTimestampField(name: String) = Field(name, FieldType.nullable(ArrowType.Timestamp(SECOND, null)), null)
}

