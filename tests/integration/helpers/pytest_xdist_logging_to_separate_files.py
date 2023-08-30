import logging
import os.path

# Makes the parallel workers of pytest-xdist to log to separate files.
# Without this function all workers will log to the same log file
# and mix everything together making it much more difficult for troubleshooting.
def setup():
    worker_name = os.environ.get("PYTEST_XDIST_WORKER", "master")
    if worker_name == "master":
        return
    logger = logging.getLogger("")
    new_handlers = []
    handlers_to_remove = []
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            filename, ext = os.path.splitext(handler.baseFilename)
            if not filename.endswith("-" + worker_name):
                new_filename = filename + "-" + worker_name
                new_handler = logging.FileHandler(new_filename + ext)
                new_handler.setFormatter(handler.formatter)
                new_handler.setLevel(handler.level)
                new_handlers.append(new_handler)
                handlers_to_remove.append(handler)
    for new_handler in new_handlers:
        logger.addHandler(new_handler)
    for handler in handlers_to_remove:
        handler.flush()
        logger.removeHandler(handler)
