-- Hashing an AggregateFunction column serializes every state. If any serialized byte fails to reach
-- the hash, every state hashes alike and counting distinct states silently collapses to one.
SELECT uniqExact(st), uniq(st)
FROM (SELECT number AS k, groupArrayState(toString(number)) AS st FROM numbers(50) GROUP BY k);
