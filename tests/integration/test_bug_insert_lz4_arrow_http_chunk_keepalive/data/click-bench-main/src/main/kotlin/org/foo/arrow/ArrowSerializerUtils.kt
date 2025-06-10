package org.foo.arrow

import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.AllocationOutcome
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.Decimal256Vector
import org.apache.arrow.vector.VarCharVector
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode

object ArrowSerializerUtils {
    private const val ONE_GIGABYTE = 3L * 1024 * 1024 * 1024

    fun rootAllocator(label: String) = RootAllocator(LoggingAllocationListener(label), ONE_GIGABYTE)

    fun BitVector.setNullableSafe(index: Int, value: Boolean?) {
        value?.let { this.setSafe(index, if (it) 1 else 0) }
    }

    fun VarCharVector.setNullableSafe(index: Int, value: String?) {
        value?.let { this.setSafe(index, it.toByteArray()) } ?: this.setNull(index)
    }

    fun Decimal256Vector.setNullableSafe(index: Int, value: BigDecimal?, scale: Int = 25) {
        value?.let { this.setSafe(index, it.setScale(scale, RoundingMode.DOWN)) } ?: this.setNull(index)
    }

}

class LoggingAllocationListener(private val level: String) : AllocationListener {
    private val logger = LoggerFactory.getLogger("AllocationListener")
    override fun onFailedAllocation(size: Long, outcome: AllocationOutcome?): Boolean {
        logger.error("Allocation failed on $level for size ${size}. $outcome")
        return super.onFailedAllocation(size, outcome)
    }
}
