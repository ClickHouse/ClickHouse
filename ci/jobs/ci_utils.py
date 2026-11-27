from ci.praktika.info import Info

EXTENDED_RUN_LABELS = {"pr-feature", "pr-improvement", "pr-performance"}
SANITIZERS = ("asan", "tsan", "msan", "ubsan")


def is_extended_run() -> bool:
    try:
        info = Info()
        return bool(set(info.pr_labels) & EXTENDED_RUN_LABELS)
    except Exception:
        return False
