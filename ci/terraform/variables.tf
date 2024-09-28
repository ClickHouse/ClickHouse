variable "region" {
  default = "us-east-1"
}

variable "sg_id" {
  default = "sg-04d02f0265bee8b24"
}

variable "ami_id" {
  default = "ami-015989dfbaffe7838"
}

variable "key_name" {
  default = "maxkey"
}

variable "runner_ci_services" {
  default = "ci_services"
}

variable "runner_ci_services_ebs" {
  default = "ci_services_ebs"
}
