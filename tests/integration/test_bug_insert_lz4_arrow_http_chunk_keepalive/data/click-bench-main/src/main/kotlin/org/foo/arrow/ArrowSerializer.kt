package org.foo.arrow

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

interface ArrowSerializer<T> {
    val schema: Schema
    fun initVectors(root : VectorSchemaRoot): ArrowVectors
    fun populate(item: T, vectors : ArrowVectors, index: Int)
}

abstract class ArrowVectors {
    abstract fun allocateNew(size: Int)
}
