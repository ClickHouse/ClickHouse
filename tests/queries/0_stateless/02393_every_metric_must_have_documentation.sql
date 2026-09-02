SET system_events_show_zero_values = true;
SELECT metric FROM system.metrics WHERE length(description) < 10;
