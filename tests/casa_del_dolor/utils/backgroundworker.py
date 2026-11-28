"""
Simple Thread worker that will do tasks in the
"""

import logging
import threading


class BackgroundWorker:
    def __init__(self, task_function, interval=1):
        """
        Initialize the background worker.

        Args:
            task_function: The function to run repeatedly in the background
            interval: Time in seconds between task executions (default: 1)
        """
        self.task_function = task_function
        self.interval = interval
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.thread = None
        self.logger = logging.getLogger(__name__)

    def set_task_function(self, _task_function):
        self.task_function = _task_function

    def worker_task(self):
        """The repetitive task that runs in the background"""
        while not self.stop_event.is_set():
            # Wait here if paused
            self.pause_event.wait()
            # Check again if we should stop (in case stop was called while paused)
            if self.stop_event.is_set():
                break
            try:
                # Execute the provided function
                self.task_function()
            except Exception as e:
                self.logger.error(str(e))

            # Sleep until interval or stop signal
            self.stop_event.wait(self.interval)

    def start(self):
        """Start the background thread"""
        # Check if thread is already running
        if self.thread and self.thread.is_alive():
            raise Exception("Background thread is already running")

        self.stop_event.clear()
        self.pause_event.set()
        self.thread = threading.Thread(target=self.worker_task, daemon=True)
        self.thread.start()
        self.logger.info("Background worker ready to take tasks")

    def pause(self):
        """Pause the background thread"""
        if not self.thread or not self.thread.is_alive():
            raise Exception("Background thread is not running")

        self.pause_event.clear()
        self.logger.info("Background worker paused")

    def resume(self):
        """Resume the background thread"""
        if not self.thread or not self.thread.is_alive():
            raise Exception("Background thread is not running")

        self.pause_event.set()
        self.logger.info("Background worker resumed")

    def stop(self):
        """Signal the background thread to stop and wait for it"""
        if not self.thread or not self.thread.is_alive():
            raise Exception("Background thread is not running")

        self.stop_event.set()
        self.pause_event.set()  # Unpause if paused, so it can exit
        self.thread.join()
        self.logger.info("Background worker stopped")

    def is_running(self):
        """Check if the background thread is currently running"""
        return self.thread is not None and self.thread.is_alive()

    def is_paused(self):
        """Check if the background thread is currently paused"""
        return not self.pause_event.is_set()
