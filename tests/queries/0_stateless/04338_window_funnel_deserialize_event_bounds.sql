-- A valid windowFunnel state (one event of condition 1) finalizes normally.
SELECT finalizeAggregation(CAST(unhex('0101000000000000000000000001') AS AggregateFunction(windowFunnel(1), UInt32, UInt8, UInt8)));

-- An event type past the number of conditions (events_size = 2) used to index events_timestamp out of bounds.
SELECT finalizeAggregation(CAST(unhex('01010000000000000000000000FF') AS AggregateFunction(windowFunnel(1), UInt32, UInt8, UInt8))); -- { serverError INCORRECT_DATA }

-- Event type 0 is a no-event sentinel only valid with strict_order; without it the index goes before the buffer.
SELECT finalizeAggregation(CAST(unhex('0101000000000000000000000000') AS AggregateFunction(windowFunnel(1), UInt32, UInt8, UInt8))); -- { serverError INCORRECT_DATA }

-- With strict_order that same event type 0 is the legitimate no-event sentinel, so the stored state must still finalize.
SELECT finalizeAggregation(CAST(unhex('0101000000000000000000000000') AS AggregateFunction(windowFunnel(1, 'strict_order'), UInt32, UInt8, UInt8)));

-- Same check for the strict_once variant, whose state carries a per-event unique id.
SELECT finalizeAggregation(CAST(unhex('01010000000000000000000000FF0100000000000000') AS AggregateFunction(windowFunnel(1, 'strict_once'), UInt32, UInt8, UInt8))); -- { serverError INCORRECT_DATA }

-- The strict_once layout also keeps event type 0 valid together with strict_order.
SELECT finalizeAggregation(CAST(unhex('01010000000000000000000000000100000000000000') AS AggregateFunction(windowFunnel(1, 'strict_order', 'strict_once'), UInt32, UInt8, UInt8)));
