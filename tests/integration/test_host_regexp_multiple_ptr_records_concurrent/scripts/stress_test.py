import pycurl
import threading
from io import BytesIO
import sys

server_ip = sys.argv[1]

mutex = threading.Lock()
success_counter = 0
number_of_threads = 1
number_of_iterations = 400


def perform_request():

    buffer = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(pycurl.INTERFACE, "192.168.0.157")
    crl.setopt(crl.WRITEDATA, buffer)
    crl.setopt(crl.URL, f"http://{server_ip}:8123/?query=select+1&user=test_dns")

    crl.perform()

    # End curl session
    crl.close()

    str_response = buffer.getvalue().decode("iso-8859-1")
    expected_response = "1\n"

    mutex.acquire()

    global success_counter

    if str_response == expected_response:
        success_counter += 1

    mutex.release()


def perform_multiple_requests(n):
    for i in range(n):
        perform_request()


threads = []


for i in range(number_of_threads):
    thread = threading.Thread(
        target=perform_multiple_requests, args=(number_of_iterations,)
    )
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

exit(success_counter == number_of_threads * number_of_iterations)
