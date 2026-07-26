-- The client does not ping the server before sending a query: it checks whether the connection is
-- still usable without a round trip, which also means it cannot mistake a slow answer for a closed
-- connection. A connection that the server really has closed must still be re-established
-- transparently. `idle_connection_timeout = 0` makes the server close the connection as soon as it
-- becomes idle, so the queries below run in a connection the client establishes on its own.

SET idle_connection_timeout = 0;

SELECT 1;
SELECT 2;
SELECT 3;
