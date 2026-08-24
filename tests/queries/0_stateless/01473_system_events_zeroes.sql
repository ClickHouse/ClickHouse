SET system_events_show_zero_values = 1;
SELECT value FROM system.events WHERE event == 'PerfAlignmentFaults';
SET system_events_show_zero_values = 0;
