-- The analyzer caches the built FunctionBase of non-deterministic functions by tree hash, so that
-- syntactically-identical calls (e.g. randConstant()) fold to the same constant. This must keep
-- working after the cache was narrowed to non-deterministic functions only (it used to apply to all
-- functions and recomputed an expensive tree hash for every function node).

SET enable_analyzer = 1;

-- Identical randConstant() calls must share the same value and therefore compare equal.
SELECT randConstant() = randConstant();
SELECT randConstant(10) = randConstant(10);

-- Sharing must survive wrapping in deterministic functions (the inner calls still share).
SELECT abs(randConstant()) = abs(randConstant());
SELECT (randConstant() + 1) = (randConstant() + 1);

-- now() is also non-deterministic and must remain consistent within the query.
SELECT now() = now();

-- Deterministic constant folding is unaffected.
SELECT 1 + 1;
