import re

def verify_warning_with_values(node, current_val, warn_val, throw_val):
    """
    Parses system.warnings to find a warning message matching the expected values.
    """
    warnings_result = node.query("SELECT message, message_format_string FROM system.warnings")
    rows = warnings_result.strip().split('\n')

    found = False
    found_message = ""

    for row in rows:
        if not row: continue
        parts = row.split('\t')
        if len(parts) < 1: continue

        message = parts[0]

        # Regex to capture values from message:
        match = re.search(
            r"\((\d+)\) exceeds the warning limit of (\d+)(?:\. You will not be able to create new .* limit of (\d+) is reached)?",
            message)

        if match:
            msg_curr = int(match.group(1))
            msg_warn = int(match.group(2))
            msg_throw = int(match.group(3)) if match.group(3) else None

            # We allow current_val to be >= expected because some internal system objects might have been attached
            if msg_curr >= current_val and msg_warn == warn_val and msg_throw == throw_val:
                found = True
                found_message = message
                break

    assert found, (
        f"Warning not found or values mismatch.\n"
        f"Expected: Current>={current_val}, Warn={warn_val}, Throw={throw_val}\n"
        f"Last found message: {found_message}"
    )


def verify_no_warning(node, message_part):
    """
    Verifies that no warning containing message_part exists in system.warnings.
    """
    warnings = node.query("SELECT message FROM system.warnings").strip()
    if not warnings:
        return
    assert message_part not in warnings, f"Found unexpected warning containing '{message_part}':\n{warnings}"
