---
toc_priority: 69
toc_title: Statistics
---

# Functions for Working with Statistics {#functions-for-working-with-statistics}

# proportionsZTest {#proportionsztest}

Applies proportion z-test to samples from two populations.

**Syntax**

``` sql
proportionsZTest(successes_x, successes_y, trials_x, trials_y, confidence_level, usevar)
```

Two sample Z test of proportions unpooled. The alternative is 'two-sided'.
