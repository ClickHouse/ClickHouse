package org.foo.domain

import com.fasterxml.uuid.Generators
import java.util.*


private val timeBasedUuid = Generators.timeBasedEpochGenerator()
fun generateId(): UUID = timeBasedUuid.generate()

@JvmInline
value class JobId(val value: UUID) {
    constructor() : this(generateId())
    fun asString() = value.toString()
}
