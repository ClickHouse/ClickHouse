
data "local_file" "user_data_ci_services" {
  filename = "${path.module}/ci_services_user_data.txt"
}
