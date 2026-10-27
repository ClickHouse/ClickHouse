#!/usr/bin/env python

"""Slack user and user group IDs for CI notifications.

These are not sensitive - any workspace member can find them via profiles or the
Slack API. What is sensitive are webhook URLs and bot tokens, which live in SSM.
"""

# User IDs
FELIXOID = "U02M9UZCEHF"

# User group IDs
CI_TEAM = "S06M1A6H482"
