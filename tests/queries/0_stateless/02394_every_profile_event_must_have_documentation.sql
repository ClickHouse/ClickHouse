SET system_events_show_zero_values = true;
SELECT event FROM system.events WHERE length(description) < 10;
