import pycurl
import threading
from io import BytesIO
import sys

mutex = threading.Lock()
client_ip = sys.argv[1]
server_ip = sys.argv[2]

success_counter = 0
number_of_threads = 1
number_of_iterations = 1

dns_google_res = "['dns.google']"
empty_res = "[]"
ns_something_res = "['NS1.Shane.co']"
excp_res = "Poco::Exception. Code: 1000, e.code() = 0, Invalid address: abcd (version 23.7.1.1)"


address_dict = {
    "2001:4860:4860::8888": dns_google_res,
    "2001:4860:4860::8844": dns_google_res,
    "": empty_res,
    "abcd": excp_res,
    "199.199.199.199": ns_something_res,
    "255.255.255.254": empty_res,
    "::ffff:118.193.34.65": empty_res,
    "::ffff:118.213.213.213": empty_res
}

def perform_request(ip_address, expected_response):
    buffer = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(pycurl.INTERFACE, client_ip)
    crl.setopt(crl.WRITEDATA, buffer)
    crl.setopt(crl.URL, f"http://{server_ip}:8123/?query=select+reverseDNSQuery('{ip_address}')")

    crl.perform()

    # End curl session
    crl.close()

    str_response = buffer.getvalue().decode("iso-8859-1")
    expected_response = expected_response + "\n"

    mutex.acquire()

    global success_counter

    if str_response == expected_response:
        success_counter += 1

    mutex.release()


def perform_multiple_requests(n):
    for _ in range(n):
        for ip_address, expected_response in address_dict.items():
            perform_request(ip_address, expected_response)


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
