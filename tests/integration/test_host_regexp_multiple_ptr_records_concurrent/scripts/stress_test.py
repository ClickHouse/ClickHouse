import pycurl
import threading
from io import BytesIO
import sys

client_ip = sys.argv[1]
server_ip = sys.argv[2]

mutex = threading.Lock()
success_counter = 0
number_of_threads = 100
number_of_iterations = 100


def perform_request():
    buffer = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(pycurl.INTERFACE, client_ip)
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
    for request_number in range(n):
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


if success_counter == number_of_threads * number_of_iterations:
    exit(0)

exit(1)
