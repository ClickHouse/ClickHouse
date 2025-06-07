package org.foo.utils

import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.random.Random


fun randomString(length: Int = 16): String {
    val chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return (1..length)
        .map { chars.random() }
        .joinToString("")
}

fun randomBoolean(): Boolean {
    return Random.nextBoolean()
}

fun randomBigDecimal(min: BigDecimal = BigDecimal.ONE, max: BigDecimal = BigDecimal("100"), scale: Int = 0): BigDecimal {
    require(min <= max) { "min must be less than or equal to max" }

    val range = max - min
    val randomFraction = BigDecimal(Random.nextDouble()).setScale(scale, RoundingMode.HALF_UP)
    return min + (range * randomFraction).setScale(scale, RoundingMode.HALF_UP)
}

fun randomStringWithPrefix(prefix: String, length: Int = 5): String {
    return "$prefix${randomString(length)}"
}
